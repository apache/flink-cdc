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

package org.apache.flink.cdc.connectors.oracle.source.assigner.splitter;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests to cover tables chunk splitter process. */
class OracleChunkSplitterTest extends OracleSourceTestBase {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeEach
    public void before() throws Exception {

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
        TestValuesTableFactory.clearAllData();

        env.setParallelism(1);
    }

    @Test
    void testIsChunkEndGeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidGeMaxCheck(a, b);
    }

    @Test
    void testIsChunkEndLeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidLeMaxCheck(a, b);
    }

    @Test
    void testGenerateSplitsForTableWithoutPrimaryKeyUsesRowIdBoundaries() throws Exception {
        createChunkKeyNoPkTable();

        OracleSourceConfig sourceConfig = createChunkKeyNoPkSourceConfig();
        OracleDialect dialect = new OracleDialect();
        TableId tableId = new TableId(null, ORACLE_SCHEMA, "CHUNK_KEY_NO_PK");
        List<SnapshotSplit> splits = generateSnapshotSplits(sourceConfig, dialect, tableId);

        assertThat(splits).hasSize(3);
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            if (split.getSplitStart() != null) {
                                assertThat(split.getSplitStart()[0]).isInstanceOf(ROWID.class);
                            }
                            if (split.getSplitEnd() != null) {
                                assertThat(split.getSplitEnd()[0]).isInstanceOf(ROWID.class);
                            }
                        });

        List<RowIdRecord> rowIdRecords = queryRowIdRecords();
        assertThat(rowIdRecords).hasSize(4);
        assertThat(rowIdRecords)
                .allSatisfy(
                        record ->
                                assertThat(splits)
                                        .filteredOn(
                                                split ->
                                                        SplitKeyUtils.splitKeyRangeContains(
                                                                new Object[] {record.rowId},
                                                                split.getSplitStart(),
                                                                split.getSplitEnd()))
                                        .as(
                                                "matching splits for id=%s rowId=%s",
                                                record.id, record.rowId)
                                        .hasSize(1));

        assertThat(querySplitScanIds(sourceConfig, tableId, splits))
                .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
    }

    @Test
    void testScanFetcherKeepsAllSnapshotRowsForTableWithoutPrimaryKey() throws Exception {
        createChunkKeyNoPkTable();

        OracleSourceConfig sourceConfig = createChunkKeyNoPkSourceConfig();
        OracleDialect dialect = new OracleDialect();
        TableId tableId = new TableId(null, ORACLE_SCHEMA, "CHUNK_KEY_NO_PK");
        List<SnapshotSplit> splits = generateSnapshotSplits(sourceConfig, dialect, tableId);
        List<Long> ids = new ArrayList<>();
        List<String> keyValues = new ArrayList<>();
        List<String> headerValues = new ArrayList<>();
        List<List<String>> afterFieldNames = new ArrayList<>();

        for (SnapshotSplit split : splits) {
            OracleSourceFetchTaskContext fetchTaskContext =
                    dialect.createFetchTaskContext(sourceConfig);
            IncrementalSourceScanFetcher fetcher =
                    new IncrementalSourceScanFetcher(fetchTaskContext, 0);
            try {
                fetcher.submitTask(new OracleScanFetchTask(split));
                while (true) {
                    Iterator<SourceRecords> recordsIterator = fetcher.pollSplitRecords();
                    if (recordsIterator == null) {
                        break;
                    }
                    while (recordsIterator.hasNext()) {
                        SourceRecords sourceRecords = recordsIterator.next();
                        for (SourceRecord record : sourceRecords.getSourceRecordList()) {
                            if (!SourceRecordUtils.isDataChangeRecord(record)) {
                                continue;
                            }
                            Struct value = (Struct) record.value();
                            Struct after = value.getStruct("after");
                            ids.add(Long.valueOf(after.getInt32("ID")));
                            keyValues.add(String.valueOf(record.key()));
                            afterFieldNames.add(
                                    after.schema().fields().stream()
                                            .map(field -> field.name())
                                            .collect(java.util.stream.Collectors.toList()));
                            List<String> headers = new ArrayList<>();
                            record.headers()
                                    .forEach(
                                            header ->
                                                    headers.add(
                                                            header.key()
                                                                    + "="
                                                                    + String.valueOf(
                                                                            header.value())));
                            headerValues.add(headers.toString());
                        }
                    }
                }
            } finally {
                fetcher.close();
            }
        }

        assertThat(ids)
                .as(
                        "snapshot ids=%s keys=%s headers=%s afterFields=%s",
                        ids, keyValues, headerValues, afterFieldNames)
                .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
    }

    private void rowidGeMaxCheck(String chunkEndStr, String maxStr) throws SQLException {
        JdbcConfiguration jdbcConfig =
                JdbcConfiguration.create()
                        .with(JdbcConfiguration.HOSTNAME, ORACLE_CONTAINER.getHost())
                        .with(JdbcConfiguration.PORT, ORACLE_CONTAINER.getOraclePort())
                        .with(JdbcConfiguration.USER, ORACLE_CONTAINER.getUsername())
                        .with(JdbcConfiguration.PASSWORD, ORACLE_CONTAINER.getPassword())
                        .with(JdbcConfiguration.DATABASE, ORACLE_CONTAINER.getDatabaseName())
                        .build();
        JdbcConnection jdbc = OracleConnectionUtils.createOracleConnection(jdbcConfig);
        ROWID chunkEnd = new ROWID(chunkEndStr);
        ROWID max = new ROWID(maxStr);
        ChunkSplitterState chunkSplitterState = new ChunkSplitterState(null, null, null);
        OracleChunkSplitter splitter = new OracleChunkSplitter(null, null, chunkSplitterState);
        assertThat(splitter.isChunkEndGeMax(jdbc, chunkEnd, max, null)).isFalse();
    }

    private void rowidLeMaxCheck(String chunkEndStr, String maxStr) throws SQLException {
        JdbcConfiguration jdbcConfig =
                JdbcConfiguration.create()
                        .with(JdbcConfiguration.HOSTNAME, ORACLE_CONTAINER.getHost())
                        .with(JdbcConfiguration.PORT, ORACLE_CONTAINER.getOraclePort())
                        .with(JdbcConfiguration.USER, ORACLE_CONTAINER.getUsername())
                        .with(JdbcConfiguration.PASSWORD, ORACLE_CONTAINER.getPassword())
                        .with(JdbcConfiguration.DATABASE, ORACLE_CONTAINER.getDatabaseName())
                        .build();
        JdbcConnection jdbc = OracleConnectionUtils.createOracleConnection(jdbcConfig);
        ROWID chunkEnd = new ROWID(chunkEndStr);
        ROWID max = new ROWID(maxStr);
        ChunkSplitterState chunkSplitterState = new ChunkSplitterState(null, null, null);
        OracleChunkSplitter splitter = new OracleChunkSplitter(null, null, chunkSplitterState);
        assertThat(splitter.isChunkEndLeMax(jdbc, chunkEnd, max, null)).isTrue();
    }

    private List<RowIdRecord> queryRowIdRecords() throws SQLException {
        List<RowIdRecord> rowIdRecords = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT ID, ROWID FROM DEBEZIUM.CHUNK_KEY_NO_PK ORDER BY ROWID")) {
            while (resultSet.next()) {
                rowIdRecords.add(
                        new RowIdRecord(
                                resultSet.getLong("ID"), (ROWID) resultSet.getObject("ROWID")));
            }
        }
        return rowIdRecords;
    }

    private List<Long> querySplitScanIds(
            OracleSourceConfig sourceConfig, TableId tableId, List<SnapshotSplit> splits)
            throws SQLException {
        JdbcConfiguration jdbcConfig = sourceConfig.getDbzConnectorConfig().getJdbcConfig();
        List<Long> ids = new ArrayList<>();
        try (JdbcConnection jdbc = OracleConnectionUtils.createOracleConnection(jdbcConfig)) {
            for (SnapshotSplit split : splits) {
                boolean isFirstSplit = split.getSplitStart() == null;
                boolean isLastSplit = split.getSplitEnd() == null;
                String sql =
                        OracleUtils.buildSplitScanQuery(
                                tableId, split.getSplitKeyType(), isFirstSplit, isLastSplit);
                try (PreparedStatement statement =
                                OracleUtils.readTableSplitDataStatement(
                                        jdbc,
                                        sql,
                                        isFirstSplit,
                                        isLastSplit,
                                        split.getSplitStart(),
                                        split.getSplitEnd(),
                                        split.getSplitKeyType().getFieldCount(),
                                        sourceConfig.getFetchSize());
                        ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        ids.add(resultSet.getLong("ID"));
                    }
                }
            }
        }
        return ids;
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

    private static class RowIdRecord {
        private final long id;
        private final ROWID rowId;

        private RowIdRecord(long id, ROWID rowId) {
            this.id = id;
            this.rowId = rowId;
        }
    }
}
