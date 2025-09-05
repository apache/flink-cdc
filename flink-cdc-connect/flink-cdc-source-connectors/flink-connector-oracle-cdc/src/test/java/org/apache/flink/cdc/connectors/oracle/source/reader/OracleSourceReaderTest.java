/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReader;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.connectors.oracle.testutils.RecordsFormatter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;

/** Tests for {@link IncrementalSourceReader}. */
public class OracleSourceReaderTest extends OracleSourceTestBase {

    @Test
    public void testFinishedUnackedSplitsCleanInvalidSplitAccordingToNewFilter() throws Exception {
        createAndInitialize("customer.sql");
        final OracleSourceConfig sourceConfig =
                getConfig(new String[] {"CUSTOMERS", "CUSTOMERS_1"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("ID", DataTypes.BIGINT()),
                        DataTypes.FIELD("NAME", DataTypes.STRING()),
                        DataTypes.FIELD("ADDRESS", DataTypes.STRING()),
                        DataTypes.FIELD("PHONE_NUMBER", DataTypes.STRING()));
        List<SourceSplitBase> snapshotSplits;
        TableId tableId = new TableId(ORACLE_DATABASE, ORACLE_SCHEMA, "CUSTOMERS");
        TableId tableId1 = new TableId(ORACLE_DATABASE, ORACLE_SCHEMA, "CUSTOMERS_1");
        OracleDialect oracleDialect = new OracleDialect();
        Map<TableId, TableChange> tableSchemas =
                oracleDialect.discoverDataCollectionSchemas(sourceConfig);

        RowType splitType =
                RowType.of(
                        new LogicalType[] {DataTypes.INT().getLogicalType()}, new String[] {"id"});
        snapshotSplits =
                Arrays.asList(
                        new SnapshotSplit(
                                tableId,
                                tableId + ":0",
                                splitType,
                                null,
                                new Integer[] {200},
                                null,
                                tableSchemas),
                        new SnapshotSplit(
                                tableId,
                                tableId + ":1",
                                splitType,
                                new Integer[] {200},
                                new Integer[] {1500},
                                null,
                                tableSchemas),
                        new SnapshotSplit(
                                tableId,
                                tableId + ":2",
                                splitType,
                                new Integer[] {1500},
                                null,
                                null,
                                tableSchemas),
                        new SnapshotSplit(
                                tableId1,
                                tableId1 + ":0",
                                splitType,
                                null,
                                new Integer[] {200},
                                null,
                                tableSchemas),
                        new SnapshotSplit(
                                tableId1,
                                tableId1 + ":1",
                                splitType,
                                new Integer[] {200},
                                new Integer[] {1500},
                                null,
                                tableSchemas),
                        new SnapshotSplit(
                                tableId1,
                                tableId1 + ":2",
                                splitType,
                                new Integer[] {1500},
                                null,
                                null,
                                tableSchemas));

        // Step 1: start source reader and assign snapshot splits
        IncrementalSourceReader<SourceRecord, JdbcSourceConfig> reader = createReader(sourceConfig);
        reader.start();
        reader.addSplits(snapshotSplits);

        String[] expectedRecords =
                new String[] {
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        // Step 2: wait the snapshot splits finished reading
        Thread.sleep(10000L);
        List<String> actualRecords = consumeRecords(reader, dataType, 42);
        assertEqualsInAnyOrder(Arrays.asList(expectedRecords), actualRecords);

        // Step 3: snapshot reader's state
        List<SourceSplitBase> splitsState = reader.snapshotState(1L);

        // Step 4: restart reader from a restored state
        final OracleSourceConfig sourceConfig1 = getConfig(new String[] {"CUSTOMERS"});
        IncrementalSourceReader<SourceRecord, JdbcSourceConfig> restartReader =
                createReader(sourceConfig1);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // Step 5: check the finished unacked splits between original reader and restarted reader
        Assertions.assertThat(restartReader.getFinishedUnackedSplits().size()).isEqualTo(3);
        reader.close();
        restartReader.close();
    }

    private IncrementalSourceReader<SourceRecord, JdbcSourceConfig> createReader(
            OracleSourceConfig configuration) {
        return createReader(configuration, new TestingReaderContext());
    }

    private IncrementalSourceReader<SourceRecord, JdbcSourceConfig> createReader(
            OracleSourceConfig configuration, SourceReaderContext readerContext) {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final SourceReaderMetricGroup sourceReaderMetricGroup = readerContext.metricGroup();
        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(sourceReaderMetricGroup);
        RedoLogOffsetFactory offsetFactory = new RedoLogOffsetFactory();
        final RecordEmitter<SourceRecords, SourceRecord, SourceSplitState> recordEmitter =
                new IncrementalSourceRecordEmitter<>(
                        new ForwardDeserializeSchema(),
                        sourceReaderMetrics,
                        configuration.isIncludeSchemaChanges(),
                        offsetFactory);
        final IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(readerContext);
        OracleDialect dialect = new OracleDialect();
        Supplier<IncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
                () ->
                        new IncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                dialect,
                                configuration,
                                incrementalSourceReaderContext,
                                SnapshotPhaseHooks.empty());
        return new IncrementalSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                readerContext.getConfiguration(),
                incrementalSourceReaderContext,
                configuration,
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                },
                dialect);
    }

    private OracleSourceConfig getConfig(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> ORACLE_SCHEMA + "." + tableName)
                        .toArray(String[]::new);
        return (OracleSourceConfig)
                new OracleSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(ORACLE_DATABASE)
                        .tableList(captureTableIds)
                        .includeSchemaChanges(false)
                        .hostname(ORACLE_CONTAINER.getHost())
                        .port(ORACLE_CONTAINER.getOraclePort())
                        .splitSize(10)
                        .fetchSize(2)
                        .username(ORACLE_CONTAINER.getUsername())
                        .password(ORACLE_CONTAINER.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .create(0);
    }

    private List<String> consumeRecords(
            IncrementalSourceReader<SourceRecord, JdbcSourceConfig> sourceReader,
            DataType recordType,
            int size)
            throws Exception {
        // Poll all the n records of the single split.
        final SimpleReaderOutput output = new SimpleReaderOutput();
        InputStatus status = MORE_AVAILABLE;
        while (MORE_AVAILABLE == status || output.getResults().size() < size) {
            status = sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    private static class SimpleReaderOutput implements ReaderOutput<SourceRecord> {

        private final List<SourceRecord> results = new ArrayList<>();

        @Override
        public void collect(SourceRecord record) {
            results.add(record);
        }

        public List<SourceRecord> getResults() {
            return results;
        }

        @Override
        public void collect(SourceRecord record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<SourceRecord> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }
}
