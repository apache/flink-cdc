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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.oracle.source.assigner.splitter.OracleChunkSplitter;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link OraclePipelineRecordEmitter}. */
class OraclePipelineRecordEmitterITCase extends OracleSourceTestBase {

    @Test
    void testEmitterKeepsAllSnapshotRowsForTableWithoutPrimaryKey() throws Exception {
        createChunkKeyNoPkTable();

        OracleSourceConfig sourceConfig = createChunkKeyNoPkSourceConfig();
        OracleDialect dialect = new OracleDialect();
        TableId splitTableId = new TableId(null, ORACLE_SCHEMA, "CHUNK_KEY_NO_PK");
        TableId recordTableId = new TableId(ORACLE_DATABASE, ORACLE_SCHEMA, "CHUNK_KEY_NO_PK");
        List<SnapshotSplit> splits = generateSnapshotSplits(sourceConfig, dialect, splitTableId);
        assertThat(splits)
                .extracting(split -> split.getSplitKeyType().getFieldNames())
                .allSatisfy(
                        fieldNames -> assertThat(fieldNames).contains(ROWID.class.getSimpleName()));

        OraclePipelineRecordEmitter emitter =
                createEmitter(sourceConfig, splitTableId, recordTableId);
        CollectingSourceOutput output = new CollectingSourceOutput();
        List<String> rawRecords = new ArrayList<>();

        for (SnapshotSplit split : splits) {
            OracleSourceFetchTaskContext fetchTaskContext =
                    dialect.createFetchTaskContext(sourceConfig);
            IncrementalSourceScanFetcher fetcher =
                    new IncrementalSourceScanFetcher(fetchTaskContext, 0);
            try {
                fetcher.submitTask(new OracleScanFetchTask(split));
                SnapshotSplitState splitState = new SnapshotSplitState(split);
                while (true) {
                    Iterator<SourceRecords> recordsIterator = fetcher.pollSplitRecords();
                    if (recordsIterator == null) {
                        break;
                    }
                    while (recordsIterator.hasNext()) {
                        SourceRecords sourceRecords = recordsIterator.next();
                        for (SourceRecord record : sourceRecords.getSourceRecordList()) {
                            if (SourceRecordUtils.isDataChangeRecord(record)
                                    && record.value() instanceof Struct
                                    && ((Struct) record.value()).schema() != null
                                    && ((Struct) record.value()).schema().field("after") != null) {
                                Struct value = (Struct) record.value();
                                Struct after = value.getStruct("after");
                                if (after != null) {
                                    rawRecords.add(
                                            String.format(
                                                    "split=%s splitType=%s start=%s end=%s id=%s headerRowId=%s",
                                                    split.splitId(),
                                                    split.getSplitKeyType().getFieldNames(),
                                                    formatBoundary(split.getSplitStart()),
                                                    formatBoundary(split.getSplitEnd()),
                                                    after.get("ID"),
                                                    extractHeaderRowId(record)));
                                }
                            }
                        }
                        emitter.emitRecord(sourceRecords, output, splitState);
                    }
                }
            } finally {
                fetcher.close();
            }
        }

        List<String> emittedRows =
                output.rows.stream()
                        .filter(row -> row.startsWith("["))
                        .collect(java.util.stream.Collectors.toList());

        assertThat(emittedRows)
                .as("emitted rows=%s raw records=%s", output.rows, rawRecords)
                .containsExactlyInAnyOrder(
                        "[1, alice, Shanghai]",
                        "[2, bob, Hangzhou]",
                        "[3, carol, Beijing]",
                        "[4, david, Shenzhen]");
    }

    private OraclePipelineRecordEmitter createEmitter(
            OracleSourceConfig sourceConfig, TableId splitTableId, TableId recordTableId) {
        SourceReaderContext readerContext = new TestingReaderContext();
        SourceReaderMetrics metrics = new SourceReaderMetrics(readerContext.metricGroup());
        HashSet<TableId> alreadySentCreateTableTables = new HashSet<>();
        alreadySentCreateTableTables.add(splitTableId);
        alreadySentCreateTableTables.add(recordTableId);

        return new OraclePipelineRecordEmitter(
                new SnapshotRowDeserializer(),
                metrics,
                false,
                new RedoLogOffsetFactory(),
                sourceConfig,
                new java.util.HashMap<>(),
                alreadySentCreateTableTables,
                false);
    }

    private void createChunkKeyNoPkTable() throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            try {
                statement.execute("DROP TABLE DEBEZIUM.CHUNK_KEY_NO_PK");
            } catch (SQLException e) {
                LOG.debug("Table DEBEZIUM.CHUNK_KEY_NO_PK does not exist before setup.", e);
            }
            statement.execute(
                    "CREATE TABLE DEBEZIUM.CHUNK_KEY_NO_PK ("
                            + "ID NUMBER(9, 0) NOT NULL,"
                            + "NAME VARCHAR2(255) NOT NULL,"
                            + "CITY VARCHAR2(255))");
            statement.execute(
                    "ALTER TABLE DEBEZIUM.CHUNK_KEY_NO_PK ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            statement.execute(
                    "INSERT INTO DEBEZIUM.CHUNK_KEY_NO_PK VALUES (1, 'alice', 'Shanghai')");
            statement.execute("INSERT INTO DEBEZIUM.CHUNK_KEY_NO_PK VALUES (2, 'bob', 'Hangzhou')");
            statement.execute(
                    "INSERT INTO DEBEZIUM.CHUNK_KEY_NO_PK VALUES (3, 'carol', 'Beijing')");
            statement.execute(
                    "INSERT INTO DEBEZIUM.CHUNK_KEY_NO_PK VALUES (4, 'david', 'Shenzhen')");
        }
    }

    private OracleSourceConfig createChunkKeyNoPkSourceConfig() {
        return (OracleSourceConfig)
                new OracleSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(ORACLE_DATABASE)
                        .tableList(ORACLE_SCHEMA + ".CHUNK_KEY_NO_PK")
                        .includeSchemaChanges(false)
                        .hostname(ORACLE_CONTAINER.getHost())
                        .port(ORACLE_CONTAINER.getOraclePort())
                        .splitSize(2)
                        .fetchSize(2)
                        .username(TOP_USER)
                        .password(TOP_SECRET)
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .create(0);
    }

    private List<SnapshotSplit> generateSnapshotSplits(
            OracleSourceConfig sourceConfig, OracleDialect dialect, TableId tableId)
            throws Exception {
        OracleChunkSplitter splitter =
                (OracleChunkSplitter)
                        dialect.createChunkSplitter(
                                sourceConfig, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        List<SnapshotSplit> splits = new ArrayList<>();

        splitter.open();
        try {
            do {
                Collection<SnapshotSplit> generatedSplits = splitter.generateSplits(tableId);
                splits.addAll(generatedSplits);
            } while (splitter.hasNextChunk());
        } finally {
            splitter.close();
        }
        return splits;
    }

    private static String formatBoundary(Object[] boundary) {
        if (boundary == null) {
            return "null";
        }
        List<String> values = new ArrayList<>(boundary.length);
        for (Object value : boundary) {
            values.add(String.valueOf(value));
        }
        return values.toString();
    }

    private static String extractHeaderRowId(SourceRecord record) {
        for (Header header : record.headers()) {
            if (ROWID.class.getSimpleName().equals(header.key()) && header.value() != null) {
                return header.value().toString();
            }
        }
        return null;
    }

    private static class SnapshotRowDeserializer
            implements org.apache.flink.cdc.debezium.DebeziumDeserializationSchema<Event> {

        @Override
        public void deserialize(SourceRecord record, Collector<Event> out) {
            Struct value = (Struct) record.value();
            Struct after = value.getStruct("after");
            if (after != null) {
                out.collect(
                        new SnapshotRowEvent(
                                after.get("ID"), after.get("NAME"), after.get("CITY")));
            }
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return TypeInformation.of(Event.class);
        }
    }

    private static class SnapshotRowEvent implements Event {
        private final Object id;
        private final Object name;
        private final Object city;

        private SnapshotRowEvent(Object id, Object name, Object city) {
            this.id = id;
            this.name = name;
            this.city = city;
        }

        @Override
        public String toString() {
            return "[" + id + ", " + name + ", " + city + "]";
        }
    }

    private static class CollectingSourceOutput implements SourceOutput<Event> {
        private final List<String> rows = new ArrayList<>();

        @Override
        public void collect(Event record) {
            rows.add(record.toString());
        }

        @Override
        public void collect(Event record, long timestamp) {
            rows.add(record.toString());
        }

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
