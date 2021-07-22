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

package com.alibaba.ververica.cdc.connectors.mysql.source;

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
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySqlSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumState;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumStateSerializer;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Supplier;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_SERVER_ID;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.getServerIdForSubTask;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithms which supports parallel
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
public class MySqlParallelSource<T>
        implements Source<T, MySqlSplit, MySqlSourceEnumState<MySqlSplit>>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final RowType splitKeyRowType;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final Configuration config;

    public MySqlParallelSource(
            RowType splitKeyRowType,
            DebeziumDeserializationSchema<T> deserializationSchema,
            Configuration config) {
        this.splitKeyRowType = splitKeyRowType;
        this.deserializationSchema = deserializationSchema;
        this.config = config;
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
        final Configuration readerConfiguration = getReaderConfig(readerContext);
        Supplier<MySqlSplitReader<MySqlSplit>> splitReaderSupplier =
                () ->
                        new MySqlSplitReader<>(
                                readerConfiguration, readerContext.getIndexOfSubtask());
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MySqlRecordEmitter<>(deserializationSchema),
                readerConfiguration,
                readerContext);
    }

    private Configuration getReaderConfig(SourceReaderContext readerContext) {
        // set the server id for each reader, will used by debezium reader
        Configuration readerConfiguration = config.clone();
        readerConfiguration.removeConfig(MySqlSourceOptions.SERVER_ID);
        readerConfiguration.setString(
                DATABASE_SERVER_ID,
                getServerIdForSubTask(config, readerContext.getIndexOfSubtask()));
        // set the DatabaseHistory name for each reader, will used by debezium reader
        readerConfiguration.setString(
                DATABASE_HISTORY_INSTANCE_NAME,
                config.toMap().get(DATABASE_HISTORY_INSTANCE_NAME)
                        + "_"
                        + readerContext.getIndexOfSubtask());
        return readerConfiguration;
    }

    @Override
    public SplitEnumerator<MySqlSplit, MySqlSourceEnumState<MySqlSplit>> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) throws Exception {
        final MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(
                        config, this.splitKeyRowType, new ArrayList<>(), new ArrayList<>());
        return new MySqlSourceEnumerator(
                enumContext, splitAssigner, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    @Override
    public SplitEnumerator<MySqlSplit, MySqlSourceEnumState<MySqlSplit>> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext,
            MySqlSourceEnumState<MySqlSplit> checkpoint)
            throws Exception {
        final MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(
                        config,
                        this.splitKeyRowType,
                        checkpoint.getAlreadyProcessedTables(),
                        checkpoint.getRemainingSplits());
        return new MySqlSourceEnumerator(
                enumContext,
                splitAssigner,
                checkpoint.getAssignedSnapshotSplits(),
                checkpoint.getAssignedBinlogSplits(),
                checkpoint.getFinishedSnapshotSplits());
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<MySqlSourceEnumState<MySqlSplit>>
            getEnumeratorCheckpointSerializer() {
        return new MySqlSourceEnumStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
