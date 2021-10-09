/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.toDebeziumConfig;
import static com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_SERVER_ID;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.getServerIdForSubTask;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change from binlog.
 *
 * <pre>
 *     1. The source supports parallel capturing table change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source does need apply any lock of MySQL.
 * </pre>
 *
 * @param <T> The record type.
 */
@Internal
public class MySqlParallelSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final Configuration flinkConf;
    private final Configuration dbzConf;
    private final String startupMode;
    private final String historyInstanceName;

    public MySqlParallelSource(
            DebeziumDeserializationSchema<T> deserializationSchema,
            Configuration flinkConf,
            Configuration dbzConf) {
        this.deserializationSchema = deserializationSchema;
        this.flinkConf = flinkConf;
        this.startupMode = flinkConf.get(SCAN_STARTUP_MODE);
        this.dbzConf = dbzConf;
        this.historyInstanceName =
                dbzConf.toMap()
                        .getOrDefault(DATABASE_HISTORY_INSTANCE_NAME, UUID.randomUUID().toString());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () ->
                        new MySqlSplitReader(
                                getReaderConfig(readerContext), readerContext.getIndexOfSubtask());
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MySqlRecordEmitter<>(deserializationSchema, sourceReaderMetrics),
                readerContext);
    }

    private io.debezium.config.Configuration getReaderConfig(SourceReaderContext readerContext) {
        // set the server id for each reader, will used by debezium reader
        Configuration readerConfiguration = dbzConf.clone();
        final Optional<String> serverId =
                getServerIdForSubTask(flinkConf, readerContext.getIndexOfSubtask());
        serverId.ifPresent(s -> readerConfiguration.setString(DATABASE_SERVER_ID, s));
        // set the DatabaseHistory name for each reader, will used by debezium reader
        readerConfiguration.setString(
                DATABASE_HISTORY_INSTANCE_NAME,
                historyInstanceName + "_" + readerContext.getIndexOfSubtask());

        return toDebeziumConfig(readerConfiguration);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {
        MySqlValidator validator = new MySqlValidator(dbzConf);
        final int currentParallelism = enumContext.currentParallelism();

        final MySqlSplitAssigner splitAssigner =
                startupMode.equals("initial")
                        ? new MySqlHybridSplitAssigner(
                                toDebeziumConfig(dbzConf),
                                currentParallelism,
                                flinkConf.getInteger(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE))
                        : new MySqlBinlogSplitAssigner(toDebeziumConfig(dbzConf));

        return new MySqlSourceEnumerator(enumContext, splitAssigner, validator);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {

        MySqlValidator validator = new MySqlValidator(dbzConf);
        final MySqlSplitAssigner splitAssigner;
        final int currentParallelism = enumContext.currentParallelism();
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            toDebeziumConfig(dbzConf),
                            currentParallelism,
                            flinkConf.getInteger(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE),
                            (HybridPendingSplitsState) checkpoint);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            toDebeziumConfig(dbzConf), (BinlogPendingSplitsState) checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }

        return new MySqlSourceEnumerator(enumContext, splitAssigner, validator);
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
