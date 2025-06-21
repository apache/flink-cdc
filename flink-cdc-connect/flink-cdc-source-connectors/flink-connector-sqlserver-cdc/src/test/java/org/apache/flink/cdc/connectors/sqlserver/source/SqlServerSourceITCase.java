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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.testutils.TestTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;
import static org.apache.flink.util.Preconditions.checkState;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** IT tests for {@link SqlServerSourceBuilder.SqlServerIncrementalSource}. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class SqlServerSourceITCase extends SqlServerSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSourceITCase.class);

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    @Test
    void testReadSingleTableWithSingleParallelism() throws Exception {
        testSqlServerParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"dbo.customers"});
    }

    @Test
    void testReadSingleTableWithMultipleParallelism() throws Exception {
        testSqlServerParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"dbo.customers"});
    }

    // Failover tests
    @Test
    void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testSqlServerParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"dbo.customers"});
    }

    @Test
    void testTaskManagerFailoverInBinlogPhase() throws Exception {
        testSqlServerParallelSource(
                FailoverType.TM, FailoverPhase.STREAM, new String[] {"dbo.customers"});
    }

    @Test
    void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testSqlServerParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"dbo.customers"});
    }

    @Test
    void testJobManagerFailoverInBinlogPhase() throws Exception {
        testSqlServerParallelSource(
                FailoverType.JM, FailoverPhase.STREAM, new String[] {"dbo.customers"});
    }

    @Test
    void testJobManagerFailoverSingleParallelism() throws Exception {
        testSqlServerParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"dbo.customers"});
    }

    @Test
    void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testSqlServerParallelSource(
                DEFAULT_PARALLELISM,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"dbo.customers"},
                true,
                RestartStrategies.fixedDelayRestart(1, 0),
                null);
    }

    @Test
    @Disabled("Disable enable backfill test until FLINK-34833 is resolved")
    void testEnableBackfillWithDMLPreHighWaterMark() throws Exception {

        List<String> records = testBackfillWhenWritingEvents(false, 25, USE_PRE_HIGHWATERMARK_HOOK);

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // In sqlserver database, because the capture process extracts change data from the
        // transaction log, there is a built-in latency between the time that a change is committed
        // to a source table and the time that the change appears within its associated change
        // table.Then in streaming phase, the log which should be ignored will be read again.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    @Disabled("Disable enable backfill test until FLINK-34833 is resolved")
    void testEnableBackfillWithDMLPostLowWaterMark() throws Exception {

        List<String> records = testBackfillWhenWritingEvents(false, 25, USE_POST_LOWWATERMARK_HOOK);

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // In sqlserver database, because the capture process extracts change data from the
        // transaction log, there is a built-in latency between the time that a change is committed
        // to a source table and the time that the change appears within its associated change
        // table.Then in streaming phase, the log which should be ignored will be read again.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSkipBackfillWithDMLPreHighWaterMark() throws Exception {

        List<String> records = testBackfillWhenWritingEvents(true, 25, USE_PRE_HIGHWATERMARK_HOOK);

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // when skip backfill, the wal log between (snapshot, high_watermark) will be seen as
        // stream event.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSkipBackfillWithDMLPostLowWaterMark() throws Exception {

        List<String> records = testBackfillWhenWritingEvents(true, 25, USE_POST_LOWWATERMARK_HOOK);

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // when skip backfill, the wal log between (snapshot, high_watermark) will still be
        // seen as stream event. This will occur data duplicate. For example, user_20 will be
        // deleted twice, and user_15213 will be inserted twice.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testTableWithChunkColumnOfNoPrimaryKey() throws Exception {
        String chunkColumn = "name";
        testSqlServerParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"dbo.customers"},
                false,
                RestartStrategies.noRestart(),
                chunkColumn);

        // since `scan.incremental.snapshot.chunk.key-column` is set, an exception should not occur.
    }

    private List<String> testBackfillWhenWritingEvents(
            boolean skipSnapshotBackfill, int fetchSize, int hookType) throws Exception {

        String databaseName = "customer";

        initializeSqlServerTable(databaseName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setParallelism(1);

        ResolvedSchema customersSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("id", BIGINT().notNull()),
                                physical("name", STRING()),
                                physical("address", STRING()),
                                physical("phone_number", STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        TestTable customerTable = new TestTable(databaseName, "dbo", "customers", customersSchema);
        String tableId = customerTable.getTableId();

        SqlServerSourceBuilder.SqlServerIncrementalSource source =
                SqlServerSourceBuilder.SqlServerIncrementalSource.<RowData>builder()
                        .hostname(MSSQL_SERVER_CONTAINER.getHost())
                        .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                        .username(MSSQL_SERVER_CONTAINER.getUsername())
                        .password(MSSQL_SERVER_CONTAINER.getPassword())
                        .databaseList(databaseName)
                        .tableList(getTableNameRegex(new String[] {"dbo.customers"}))
                        .deserializer(customerTable.getDeserializer())
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .build();

        // Do some database operations during hook in snapshot period.
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        String[] statements =
                new String[] {
                    String.format(
                            "INSERT INTO %s VALUES (15213, 'user_15213', 'Shanghai', '123567891234')",
                            tableId),
                    String.format("UPDATE %s SET address='Pittsburgh' WHERE id=2000", tableId),
                    String.format("DELETE FROM %s WHERE id=1019", tableId)
                };
        SnapshotPhaseHook snapshotPhaseHook =
                (sourceConfig, split) -> {
                    SqlServerDialect dialect =
                            new SqlServerDialect((SqlServerSourceConfig) sourceConfig);
                    try (JdbcConnection sqlServerConnection =
                            dialect.openJdbcConnection((JdbcSourceConfig) sourceConfig)) {
                        sqlServerConnection.execute(statements);
                        sqlServerConnection.commit();
                    }
                };

        if (hookType == USE_POST_LOWWATERMARK_HOOK) {
            hooks.setPostLowWatermarkAction(snapshotPhaseHook);
        } else if (hookType == USE_PRE_HIGHWATERMARK_HOOK) {
            hooks.setPreHighWatermarkAction(snapshotPhaseHook);
        }
        source.setSnapshotHooks(hooks);

        List<String> records = new ArrayList<>();
        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Backfill Skipped Source")
                        .executeAndCollect()) {
            records = fetchRowData(iterator, fetchSize, customerTable::stringify);
            env.close();
        }
        return records;
    }

    private void testSqlServerParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testSqlServerParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testSqlServerParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testSqlServerParallelSource(
                parallelism,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                false,
                RestartStrategies.fixedDelayRestart(1, 0),
                null);
    }

    private void testSqlServerParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            boolean skipSnapshotBackfill,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
            String chunkColumn)
            throws Exception {

        String databaseName = "customer";

        initializeSqlServerTable(databaseName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(restartStrategyConfiguration);

        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '4',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + "%s"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        databaseName,
                        getTableNameRegex(captureCustomerTables),
                        skipSnapshotBackfill,
                        chunkColumn == null
                                ? ""
                                : ",'scan.incremental.snapshot.chunk.key-column'='"
                                        + chunkColumn
                                        + "'");

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
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
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(100));
        }

        LOG.info("snapshot data start");
        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the change stream data
        for (String tableId : captureCustomerTables) {
            makeFirstPartChangeStreamEvents(databaseName + "." + tableId);
        }
        if (failoverPhase == FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartBinlogEvents(databaseName + "." + tableId);
        }

        String[] binlogForSingleTable =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(Arrays.asList(binlogForSingleTable));
        }
        assertEqualsInAnyOrder(expectedBinlogData, fetchRows(iterator, expectedBinlogData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private void makeFirstPartChangeStreamEvents(String tableId) {
        executeSql("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103");
        executeSql("DELETE FROM " + tableId + " where id = 102");
        executeSql("INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')");
        executeSql("UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
    }

    private void makeSecondPartBinlogEvents(String tableId) {
        executeSql("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
        executeSql("INSERT INTO " + tableId + " VALUES(2001, 'user_22','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2002, 'user_23','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2003, 'user_24','Shanghai','123567891234')");
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    public static List<String> fetchRowData(
            Iterator<RowData> iter, int size, Function<RowData, String> stringifier) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return rows.stream().map(stringifier).collect(Collectors.toList());
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, ","));
        }
    }
}
