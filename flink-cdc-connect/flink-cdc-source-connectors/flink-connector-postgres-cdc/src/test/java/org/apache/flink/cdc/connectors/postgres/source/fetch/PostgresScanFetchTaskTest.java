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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.postgres.testutils.TestTableId;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.google.protobuf.ByteString;
import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;
import io.debezium.connector.postgresql.connection.pgproto.PgProtoColumnValue;
import io.debezium.connector.postgresql.proto.PgProto;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.core.BaseConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static io.debezium.connector.postgresql.connection.ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Tests for {@link PostgresScanFetchTask}. */
class PostgresScanFetchTaskTest extends PostgresTestBase {
    protected static final int DEFAULT_PARALLELISM = 4;
    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private static final String schemaName = "customer";
    private static final String tableName = "Customers";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @BeforeEach
    void before() throws SQLException {
        customDatabase.createAndInitialize();
        customDatabase.createPublication(DEFAULT_PUBLICATION_NAME);
    }

    private final UniqueDatabase historicalDatesDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "historical_dates",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    void testChangingDataInSnapshotScan() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, hangzhou, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testInsertDataInSnapshotScan() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] insertDataSql =
                new String[] {
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(112, 'user_12','Shanghai','123567891234')",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(113, 'user_13','Shanghai','123567891234')",
                };
        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[112, user_12, Shanghai, 123567891234]",
                    "+I[113, user_13, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };
        List<String> actual =
                getDataInSnapshotScan(
                        insertDataSql, schemaName, tableName, USE_POST_LOWWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testDeleteDataInSnapshotScan() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 101",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                };
        String[] expected =
                new String[] {
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };
        List<String> actual =
                getDataInSnapshotScan(
                        deleteDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testSnapshotScanSkipBackfillWithPostLowWatermark() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, hangzhou, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        // Change data during [low_watermark, snapshot) will not be captured by snapshotting
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_POST_LOWWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testSnapshotScanSkipBackfillWithPreHighWatermark() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        // Change data during [snapshot, high_watermark) will not be captured by snapshotting
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testMultipleSplitsWithBackfill() throws Exception {
        TestTableId tableId = new TestTableId(schemaName, tableName);
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, null, 4, false);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));

        SnapshotPhaseHooks snapshotHooks = new SnapshotPhaseHooks();
        snapshotHooks.setPreHighWatermarkAction(
                (postgresSourceConfig, split) -> {
                    try (PostgresConnection conn = postgresDialect.openJdbcConnection()) {
                        conn.execute(
                                "UPDATE "
                                        + tableId.toSql()
                                        + " SET address = 'Beijing' WHERE \"Id\" = 103");
                        conn.commit();
                    }
                });

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("Id", DataTypes.BIGINT()),
                        DataTypes.FIELD("Name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));

        PostgresSourceFetchTaskContext postgresSourceFetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, postgresDialect);

        List<String> actual =
                readTableSnapshotSplits(
                        reOrderSnapshotSplits(snapshotSplits),
                        postgresSourceFetchTaskContext,
                        snapshotSplits.size(),
                        dataType,
                        snapshotHooks);

        // Verify the ScanFetcher can successfully process all splits without getting stuck
        // (the FLINK-39207 bug would cause the reader to appear finished/stuck
        // when reusing a stopped ScanFetcher for the next split).
        // The preHighWatermark hook forces backfill phase for each split by making
        // highWatermark > lowWatermark.
        assertThat(actual).hasSize(21);
        assertThat(actual).contains("+I[103, user_3, Beijing, 123567891234]");
    }

    @Test
    void testSnapshotFetchSize() throws Exception {
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, null, 10, true);
        sourceConfigFactory.fetchSize(2);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));
        SnapshotSplit snapshotSplit = getSnapshotSplits(sourceConfig, postgresDialect).get(0);
        final String selectSql =
                PostgresQueryUtils.buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null,
                        Collections.emptyList());

        try (PostgresConnection jdbcConnection = postgresDialect.openJdbcConnection();
                PreparedStatement selectStatement =
                        PostgresQueryUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType().getFieldCount(),
                                sourceConfig.getFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {
            assertThat(rs.getFetchSize()).isEqualTo(2);
        }
    }

    @Test
    void testHistoricalDatesInSnapshotScan() throws Exception {
        // Pre-fix, dates before 1582-10-15 are shifted by N days (e.g. 0001-01-01 by 2)
        // because the snapshot path's rs.getObject() goes through GregorianCalendar.
        // Post-fix, getColumnValue() reads via java.time types and is calendar-clean.
        historicalDatesDatabase.createAndInitialize();
        historicalDatesDatabase.createPublication(DEFAULT_PUBLICATION_NAME);

        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        historicalDatesDatabase,
                        "historical_dates",
                        "date_boundary",
                        null,
                        10,
                        false);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, postgresDialect);
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);

        List<SourceRecord> rawRecords =
                readRawSnapshotRecords(
                        reOrderSnapshotSplits(snapshotSplits),
                        taskContext,
                        1,
                        new SnapshotPhaseHooks());

        Map<Integer, Struct> afterById = new HashMap<>();
        for (SourceRecord r : rawRecords) {
            if (!SourceRecordUtils.isDataChangeRecord(r)) {
                continue;
            }
            Struct after = ((Struct) r.value()).getStruct("after");
            afterById.put(after.getInt32("id"), after);
        }

        // Debezium emits TIMESTAMP as MicroTimestamp (long micros from epoch UTC),
        // TIMESTAMPTZ as ZonedTimestamp (ISO string), DATE as days from epoch.
        assertHistoricalRow(
                afterById.get(1),
                "0001-01-01T00:00:00",
                "0001-01-01T00:00:00Z",
                LocalDate.of(1, 1, 1));
        assertHistoricalRow(
                afterById.get(2),
                "1582-10-04T00:00:00",
                "1582-10-04T00:00:00Z",
                LocalDate.of(1582, 10, 4));
        assertHistoricalRow(
                afterById.get(3),
                "1582-10-15T00:00:00",
                "1582-10-15T00:00:00Z",
                LocalDate.of(1582, 10, 15));
        assertHistoricalRow(
                afterById.get(4),
                "1900-12-31T23:59:59.123456",
                "1900-12-31T23:59:59.123456Z",
                LocalDate.of(1900, 12, 31));
        assertHistoricalRow(
                afterById.get(5),
                "1901-01-02T00:00:00",
                "1901-01-02T00:00:00Z",
                LocalDate.of(1901, 1, 2));
    }

    @Test
    void testDecoderbufsTimestampArray() throws Exception {
        PostgresSourceConfigFactory factory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, null, 10, false);
        try (PostgresConnection connection =
                new PostgresDialect(factory.create(0)).openJdbcConnection()) {
            TypeRegistry types = connection.getTypeRegistry();
            PgProto.DatumMessage datum =
                    PgProto.DatumMessage.newBuilder()
                            .setColumnName("ts")
                            .setColumnType(PgOid.TIMESTAMP_ARRAY)
                            .setDatumBytes(
                                    ByteString.copyFromUtf8(
                                            "{\"0001-02-29 12:34:56.123456 BC\",NULL}"))
                            .build();
            Object result =
                    ReplicationMessageColumnValueResolver.resolveValue(
                            "ts",
                            types.get(PgOid.TIMESTAMP_ARRAY),
                            "timestamp[]",
                            new PgProtoColumnValue(datum),
                            () -> connection.connection().unwrap(BaseConnection.class),
                            false,
                            types);
            assertThat(result)
                    .isEqualTo(
                            Arrays.asList(LocalDateTime.parse("0000-02-29T12:34:56.123456"), null));
        }
    }

    @Test
    void testHistoricalTimestampArrayInSnapshotAndStreaming() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            verifySnapshotAndStreaming("timestamp_array");
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"infinity_boundary", "bc_boundary"})
    void testTemporalBoundariesInSnapshotAndStreaming(String tableName) throws Exception {
        verifySnapshotAndStreaming(tableName);
    }

    private void verifySnapshotAndStreaming(String tableName) throws Exception {
        historicalDatesDatabase.createAndInitialize();
        historicalDatesDatabase.createPublication(DEFAULT_PUBLICATION_NAME);
        String slotName = getSlotName();
        PostgresSourceConfigFactory factory =
                getMockPostgresSourceConfigFactory(
                        historicalDatesDatabase,
                        "historical_dates",
                        tableName,
                        slotName,
                        100,
                        false);
        PostgresSourceConfig config = factory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        List<SnapshotSplit> splits = getSnapshotSplits(config, dialect);
        Map<Integer, Struct> snapshot = new HashMap<>();
        collectTemporalRows(
                readRawSnapshotRecords(
                        splits,
                        new PostgresSourceFetchTaskContext(config, dialect),
                        splits.size(),
                        new SnapshotPhaseHooks()),
                snapshot);
        assertTemporalRows(snapshot, tableName);

        factory.startupOptions(StartupOptions.latest());
        config = factory.create(0);
        dialect = new PostgresDialect(config);
        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(config, dialect);
        StreamSplitAssigner assigner =
                new StreamSplitAssigner(
                        config,
                        dialect,
                        new PostgresOffsetFactory(),
                        new MockSplitEnumeratorContext<>(1));
        assigner.open();
        StreamSplit split =
                StreamSplit.fillTableSchemas(
                        assigner.createStreamSplit(),
                        dialect.discoverDataCollectionSchemas(config));
        IncrementalSourceStreamFetcher fetcher = new IncrementalSourceStreamFetcher(context, 0);
        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            fetcher.submitTask(dialect.createFetchTask(split));
            await().atMost(Duration.ofSeconds(30))
                    .until(
                            () ->
                                    connection.queryAndMap(
                                            "SELECT active FROM pg_replication_slots WHERE slot_name = '"
                                                    + slotName
                                                    + "'",
                                            rs -> rs.next() && rs.getBoolean(1)));
            connection.execute("UPDATE historical_dates." + tableName + " SET ts = ts");
            connection.commit();
            Map<Integer, Struct> streaming = new HashMap<>();
            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(
                            () -> {
                                Iterator<SourceRecords> batches = fetcher.pollSplitRecords();
                                if (batches != null) {
                                    while (batches.hasNext()) {
                                        collectTemporalRows(
                                                batches.next().getSourceRecordList(), streaming);
                                    }
                                }
                                assertThat(streaming).hasSize(snapshot.size());
                            });
            assertTemporalRows(streaming, tableName);
            assertThat(streaming).isEqualTo(snapshot);
        } finally {
            fetcher.close();
        }
    }

    private static void collectTemporalRows(List<SourceRecord> records, Map<Integer, Struct> rows) {
        for (SourceRecord record : records) {
            if (SourceRecordUtils.isDataChangeRecord(record)) {
                Struct after = ((Struct) record.value()).getStruct("after");
                rows.put(after.getInt32("id"), after);
            }
        }
    }

    private static void assertTemporalRows(Map<Integer, Struct> rows, String tableName) {
        if (tableName.equals("timestamp_array")) {
            assertThat(rows).hasSize(3);
            assertThat(rows.get(1).getArray("ts"))
                    .containsExactly(
                            -2208988799876544L,
                            micros("0000-01-01T00:00:00"),
                            micros("0000-02-29T12:34:56.123456"),
                            null,
                            9223372036825200000L,
                            -9223372036832400000L);
            assertThat(rows.get(1).getArray("tstz"))
                    .containsExactly("0000-02-29T04:34:56.123456Z", null, "infinity", "-infinity");
            assertThat(rows.get(1).getArray("d"))
                    .containsExactly(
                            (int) LocalDate.of(0, 1, 1).toEpochDay(),
                            (int) LocalDate.of(0, 2, 29).toEpochDay(),
                            null,
                            -2147472692,
                            -2147472691);
            for (String column : Arrays.asList("ts", "tstz", "d")) {
                assertThat(rows.get(2).getArray(column)).isEmpty();
                assertThat(rows.get(3).get(column)).isNull();
            }
        } else if (tableName.equals("infinity_boundary")) {
            assertThat(rows).hasSize(3);
            assertThat(rows.get(1).get("ts")).isEqualTo(9223372036825200000L);
            assertThat(rows.get(1).get("tstz")).isEqualTo("infinity");
            assertThat(rows.get(1).get("d")).isEqualTo(-2147472692);
            assertThat(rows.get(2).get("ts")).isEqualTo(-9223372036832400000L);
            assertThat(rows.get(2).get("tstz")).isEqualTo("-infinity");
            assertThat(rows.get(2).get("d")).isEqualTo(-2147472691);
            assertThat(rows.get(3).get("ts")).isNull();
            assertThat(rows.get(3).get("tstz")).isNull();
            assertThat(rows.get(3).get("d")).isNull();
        } else {
            assertThat(rows).hasSize(5);
            assertHistoricalRow(
                    rows.get(1),
                    "0000-02-29T12:34:56.123456",
                    "0000-02-29T04:34:56.123456Z",
                    LocalDate.of(0, 2, 29));
            assertHistoricalRow(
                    rows.get(2),
                    "-0004-02-29T00:00:00",
                    "-0004-02-29T00:00:00Z",
                    LocalDate.of(-4, 2, 29));
            assertHistoricalRow(
                    rows.get(3),
                    "0000-02-28T00:00:00",
                    "0000-02-28T00:00:00Z",
                    LocalDate.of(0, 2, 28));
            assertHistoricalRow(
                    rows.get(4),
                    "0000-03-01T00:00:00",
                    "0000-03-01T00:00:00Z",
                    LocalDate.of(0, 3, 1));
            assertHistoricalRow(
                    rows.get(5),
                    "0004-02-29T00:00:00",
                    "0004-02-29T00:00:00Z",
                    LocalDate.of(4, 2, 29));
        }
    }

    private static void assertHistoricalRow(
            Struct after, String expectedTsIso, String expectedTstz, LocalDate expectedDate) {
        assertThat(after.get("ts")).isEqualTo(micros(expectedTsIso));
        assertThat(after.get("tstz")).isEqualTo(expectedTstz);
        assertThat(after.get("d")).isEqualTo((int) expectedDate.toEpochDay());
    }

    private static long micros(String iso) {
        LocalDateTime value = LocalDateTime.parse(iso);
        return value.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + value.getNano() / 1_000L;
    }

    private List<SourceRecord> readRawSnapshotRecords(
            List<SnapshotSplit> snapshotSplits,
            PostgresSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            SnapshotPhaseHooks snapshotPhaseHooks)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);
        List<SourceRecord> result = new ArrayList<>();
        try {
            for (int i = 0; i < scanSplitsNum; i++) {
                SnapshotSplit sqlSplit = snapshotSplits.get(i);
                if (sourceScanFetcher.isFinished()) {
                    FetchTask<SourceSplitBase> fetchTask =
                            taskContext.getDataSourceDialect().createFetchTask(sqlSplit);
                    ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotPhaseHooks);
                    sourceScanFetcher.submitTask(fetchTask);
                }
                Iterator<SourceRecords> res;
                while ((res = sourceScanFetcher.pollSplitRecords()) != null) {
                    while (res.hasNext()) {
                        result.addAll(res.next().getSourceRecordList());
                    }
                }
            }
        } finally {
            sourceScanFetcher.close();
        }
        return result;
    }

    private List<String> getDataInSnapshotScan(
            String[] changingDataSql,
            String schemaName,
            String tableName,
            int hookType,
            boolean skipSnapshotBackfill)
            throws Exception {
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, null, 10, skipSnapshotBackfill);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, postgresDialect);
        try (PostgresConnection postgresConnection = postgresDialect.openJdbcConnection()) {
            SnapshotPhaseHook snapshotPhaseHook =
                    (postgresSourceConfig, split) -> {
                        postgresConnection.execute(changingDataSql);
                        postgresConnection.commit();
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    };

            if (hookType == USE_POST_LOWWATERMARK_HOOK) {
                hooks.setPostLowWatermarkAction(snapshotPhaseHook);
            } else if (hookType == USE_PRE_HIGHWATERMARK_HOOK) {
                hooks.setPreHighWatermarkAction(snapshotPhaseHook);
            }

            final DataType dataType =
                    DataTypes.ROW(
                            DataTypes.FIELD("Id", DataTypes.BIGINT()),
                            DataTypes.FIELD("Name", DataTypes.STRING()),
                            DataTypes.FIELD("address", DataTypes.STRING()),
                            DataTypes.FIELD("phone_number", DataTypes.STRING()));

            PostgresSourceFetchTaskContext postgresSourceFetchTaskContext =
                    new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);

            return readTableSnapshotSplits(
                    reOrderSnapshotSplits(snapshotSplits),
                    postgresSourceFetchTaskContext,
                    1,
                    dataType,
                    hooks);
        }
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            PostgresSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            DataType dataType,
            SnapshotPhaseHooks snapshotPhaseHooks)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            SnapshotSplit sqlSplit = snapshotSplits.get(i);
            if (sourceScanFetcher.isFinished()) {
                FetchTask<SourceSplitBase> fetchTask =
                        taskContext.getDataSourceDialect().createFetchTask(sqlSplit);
                ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotPhaseHooks);
                sourceScanFetcher.submitTask(fetchTask);
            }
            Iterator<SourceRecords> res;
            while ((res = sourceScanFetcher.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecords sourceRecords = res.next();
                    result.addAll(sourceRecords.getSourceRecordList());
                }
            }
        }

        sourceScanFetcher.close();

        assertThat(sourceScanFetcher.getExecutorService()).isNotNull();
        assertThat(sourceScanFetcher.getExecutorService().isTerminated()).isTrue();

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            PostgresSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect)
            throws Exception {
        List<io.debezium.relational.TableId> discoverTables =
                sourceDialect.discoverDataCollections(sourceConfig);
        OffsetFactory offsetFactory = new PostgresOffsetFactory();
        final SnapshotSplitAssigner snapshotSplitAssigner =
                new SnapshotSplitAssigner<JdbcSourceConfig>(
                        sourceConfig,
                        DEFAULT_PARALLELISM,
                        discoverTables,
                        sourceDialect.isDataCollectionIdCaseSensitive(sourceConfig),
                        sourceDialect,
                        offsetFactory);
        snapshotSplitAssigner.initEnumeratorMetrics(
                new SourceEnumeratorMetrics(
                        UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup()));
        snapshotSplitAssigner.open();
        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        Optional<SourceSplitBase> split = snapshotSplitAssigner.getNext();
        while (split.isPresent()) {
            snapshotSplitList.add(split.get().asSnapshotSplit());
            split = snapshotSplitAssigner.getNext();
        }

        snapshotSplitAssigner.close();
        return snapshotSplitList;
    }

    // Due to the default enabling of scan.incremental.snapshot.unbounded-chunk-first.enabled,
    // the split order becomes [end,null], [null,start], ... which is different from the original
    // order.
    // The first split in the list is actually the last unbounded split that should be at the end.
    // This method adjusts the order to restore the original sequence: [null,start], ...,
    // [end,null],
    // ensuring the correctness of test cases.
    private List<SnapshotSplit> reOrderSnapshotSplits(List<SnapshotSplit> snapshotSplits) {
        if (snapshotSplits.size() > 1) {
            SnapshotSplit firstSplit = snapshotSplits.get(0);
            snapshotSplits.remove(0);
            snapshotSplits.add(firstSplit);
        }
        return snapshotSplits;
    }
}
