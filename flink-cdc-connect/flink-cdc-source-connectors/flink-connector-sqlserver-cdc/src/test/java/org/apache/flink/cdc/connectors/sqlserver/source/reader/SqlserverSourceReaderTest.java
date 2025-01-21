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

package org.apache.flink.cdc.connectors.sqlserver.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceTestBase;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.sqlserver.testutils.RecordsFormatter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;
import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;
import static org.junit.Assert.assertEquals;

/** Tests for Sqlserver incremental source reader. */
public class SqlserverSourceReaderTest extends SqlServerSourceTestBase {

    @Test
    public void testIncrementalReadFailoverCrossTransaction() throws Exception {

        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        final SqlServerSourceConfig sourceConfig =
                SqlServerTestBase.getConfigFactory(databaseName, new String[] {tableName}, 1)
                        .create(0);

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));

        IncrementalSourceReaderWithCommit reader =
                createReader(
                        sourceConfig, new TestingReaderContext(), SnapshotPhaseHooks.empty(), 1);
        reader.start();

        SqlServerDialect dialect = new SqlServerDialect(sourceConfig);

        Map<TableId, TableChanges.TableChange> tableIdTableChangeMap =
                dialect.discoverDataCollectionSchemas(sourceConfig);

        // region wait SQL server's cdc log to be ready
        Offset offset =
                Awaitility.await()
                        .timeout(Duration.ofSeconds(60))
                        .until(
                                () -> dialect.displayCurrentOffset(sourceConfig),
                                o -> !o.getOffset().isEmpty());
        // endregion

        StreamSplit streamSplit =
                new StreamSplit(
                        STREAM_SPLIT_ID,
                        new LsnFactory().newOffset(offset.getOffset()),
                        new LsnFactory().createNoStoppingOffset(),
                        new ArrayList<>(),
                        tableIdTableChangeMap,
                        0,
                        false,
                        true);

        reader.addSplits(Collections.singletonList(streamSplit));

        log.info("making changes");
        // step-1: make 6 change events in one SQL server transaction
        makeChangeEventsInOneTransaction(databaseName + "." + tableName);

        // step-2: fetch the first 2 records belong to the SQL server transaction
        String[] expectedRecords =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]"
                };
        // the 2 records are produced by 1 operations
        List<String> actualRecords = consumeRecords(reader, dataType);
        assertEqualsInOrder(Arrays.asList(expectedRecords), actualRecords);
        // check the binlog split state
        List<SourceSplitBase> splitsState = reader.snapshotState(1L);

        assertEquals(1, splitsState.size());
        reader.close();

        // step-3: mock failover from a restored state
        IncrementalSourceReaderWithCommit restartReader =
                createReader(
                        sourceConfig, new TestingReaderContext(), SnapshotPhaseHooks.empty(), 10);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // step-4: fetch the rest 4 records belong to the SQL server transaction
        String[] expectedRestRecords =
                new String[] {
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]"
                };

        // the 4 records are produced by 3 operations
        List<String> restRecords = consumeRecords(restartReader, dataType);
        assertEqualsInOrder(Arrays.asList(expectedRestRecords), restRecords);
        restartReader.close();
    }

    private void makeChangeEventsInOneTransaction(String tableId) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            // make 6 change events by 4 operations
            statement.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103");
            statement.execute("DELETE FROM " + tableId + " where id = 102");
            statement.execute(
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')");
            statement.execute("UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();
        }
    }

    private List<String> consumeRecords(
            IncrementalSourceReaderWithCommit sourceReader, DataType recordType) throws Exception {
        // Poll all the n records of the single split.
        final SimpleReaderOutput output = new SimpleReaderOutput();
        InputStatus status;
        do {
            status = sourceReader.pollNext(output);
        } while (MORE_AVAILABLE == status || output.getResults().isEmpty());
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    public IncrementalSourceReaderWithCommit createReader(
            SqlServerSourceConfig sourceConfig,
            SourceReaderContext readerContext,
            SnapshotPhaseHooks snapshotHooks,
            int limit)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(readerContext);

        Supplier splitReaderSupplier =
                () ->
                        new IncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                new SqlServerDialect(sourceConfig),
                                sourceConfig,
                                incrementalSourceReaderContext,
                                snapshotHooks);

        final RecordEmitter recordEmitter =
                new SqlserverLimitRecordEmitter<>(
                        new ForwardDeserializeSchema(),
                        new SourceReaderMetrics(readerContext.metricGroup()),
                        sourceConfig.isIncludeSchemaChanges(),
                        new LsnFactory(),
                        limit);

        return new IncrementalSourceReaderWithCommit(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                readerContext.getConfiguration(),
                incrementalSourceReaderContext,
                sourceConfig,
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return new LsnFactory();
                    }
                },
                new SqlServerDialect(sourceConfig));
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
        public SourceOutput<SourceRecord> createOutputForSplit(java.lang.String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(java.lang.String splitId) {}
    }

    private static class ForwardDeserializeSchema<T> implements DebeziumDeserializationSchema<T> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
            out.collect((T) record);
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of((Class<T>) SourceRecord.class);
        }
    }

    /**
     * A implementation of {@link RecordEmitter} which only emit records in given limit number, this
     * class is used for test purpose.
     */
    private static class SqlserverLimitRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

        private static final Logger LOG =
                LoggerFactory.getLogger(SqlserverLimitRecordEmitter.class);

        private final int limit;

        private SqlserverLimitRecordEmitter(
                DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
                SourceReaderMetrics sourceReaderMetrics,
                boolean includeSchemaChanges,
                OffsetFactory offsetFactory,
                int limit) {
            super(
                    debeziumDeserializationSchema,
                    sourceReaderMetrics,
                    includeSchemaChanges,
                    offsetFactory);
            this.limit = limit;
        }

        @Override
        public void emitRecord(
                SourceRecords sourceRecords, SourceOutput<T> output, SourceSplitState splitState)
                throws Exception {
            int sendCnt = 0;
            final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
            while (elementIterator.hasNext()) {
                if (sendCnt >= limit) {
                    break;
                }
                SourceRecord next = elementIterator.next();
                LOG.debug("SqlserverLimitRecordEmitter emit record: {}", next);
                processElement(next, output, splitState);
                sendCnt++;
            }
        }
    }
}
