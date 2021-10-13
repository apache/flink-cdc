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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
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
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.function.Supplier;

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
 * <pre>{@code
 * MySqlSource
 *     .<RowData>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MySqlSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
@Internal
public class MySqlSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final MySqlSourceConfigFactory configFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;

    /**
     * Get a MySqlParallelSourceBuilder to build a {@link MySqlSource}.
     *
     * @return a MySql parallel source builder.
     */
    @PublicEvolving
    public static <T> MySqlSourceBuilder<T> builder() {
        return new MySqlSourceBuilder<>();
    }

    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MySqlSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () -> new MySqlSplitReader(sourceConfig, readerContext.getIndexOfSubtask());
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MySqlRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges()),
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {
        final int currentParallelism = enumContext.currentParallelism();
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        // check the valid for server id range and parallelism.
        if (sourceConfig.getServerIdRange() != null
                && sourceConfig.getServerIdRange().getNumberOfServerIds() < currentParallelism) {
            throw new IllegalArgumentException(
                    String.format(
                            "The server id range [%s] is not enough for current source parallelism %s, "
                                    + "please adjust the server id range to make the number of server id is "
                                    + "larger than the source parallelism.",
                            sourceConfig.getServerIdRange(), currentParallelism));
        }

        MySqlValidator validator = new MySqlValidator(sourceConfig.getDbzProperties());
        final MySqlSplitAssigner splitAssigner =
                StartupMode.INITIAL == sourceConfig.getStartupOptions().startupMode
                        ? new MySqlHybridSplitAssigner(sourceConfig, currentParallelism)
                        : new MySqlBinlogSplitAssigner(sourceConfig);

        return new MySqlSourceEnumerator(enumContext, splitAssigner, validator);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        MySqlValidator validator = new MySqlValidator(sourceConfig.getDbzProperties());
        final MySqlSplitAssigner splitAssigner;
        final int currentParallelism = enumContext.currentParallelism();
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            sourceConfig,
                            currentParallelism,
                            (HybridPendingSplitsState) checkpoint);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            sourceConfig, (BinlogPendingSplitsState) checkpoint);
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
