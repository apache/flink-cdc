/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import com.ververica.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import com.ververica.cdc.connectors.oracle.testutils.TestTable;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests for {@link OracleSourceBuilder.OracleIncrementalSource}. */
public class OracleSourceITCase extends OracleSourceTestBase {
    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceITCase.class);

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testOracleParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"CUSTOMERS"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testTaskManagerFailoverInRedoLogPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.TM, FailoverPhase.REDO_LOG, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInRedoLogPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.JM, FailoverPhase.REDO_LOG, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testOracleParallelSource(
                DEFAULT_PARALLELISM,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"CUSTOMERS"},
                true);
    }

    @Test
    public void testEnableBackfillWithPreHighWaterMark() throws Exception {

        List<String> records = getResultOfWithHooks(false, 21, USE_PRE_HIGHWATERMARK_HOOK);

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
                        "+I[15213, user_15213, Shanghai, 123567891234]");
        // when enable backfill, the wal log between [snapshot_point,  high_watermark) will be
        // applied as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testEnableBackfillWithPostLowWaterMark() throws Exception {
        List<String> records = getResultOfWithHooks(false, 21, USE_POST_LOWWATERMARK_HOOK);

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
                        "+I[15213, user_15213, Shanghai, 123567891234]");
        // when enable backfill, the wal log between [low_watermark, snapshot_point) will be applied
        // as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testSkipBackfillWithPreHighWaterMark() throws Exception {
        List<String> records = getResultOfWithHooks(true, 25, USE_PRE_HIGHWATERMARK_HOOK);

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
        // when skip backfill, the wal log between [snapshot_point,  high_watermark) will be seen as
        // stream event.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testSkipBackfillWithPostLowWaterMark() throws Exception {

        List<String> records = getResultOfWithHooks(true, 25, USE_POST_LOWWATERMARK_HOOK);

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
        // when skip backfill, the wal log between [snapshot_point,  high_watermark) will still be
        // seen as stream event. This will occur data duplicate. For example, user_20 will be
        // deleted twice, and user_15213 will be inserted twice.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    private List<String> getResultOfWithHooks(
            boolean skipSnapshotBackfill, int fetchSize, int hookType) throws Exception {
        createAndInitialize("customer.sql");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200L);
        env.setParallelism(1);

        ResolvedSchema customersSchame =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("ID", BIGINT().notNull()),
                                physical("NAME", STRING()),
                                physical("ADDRESS", STRING()),
                                physical("PHONE_NUMBER", STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("ID")));
        TestTable customerTable =
                new TestTable(ORACLE_DATABASE, ORACLE_SCHEMA, "CUSTOMERS", customersSchame);
        String tableId = customerTable.getTableId();

        OracleSourceBuilder.OracleIncrementalSource source =
                OracleSourceBuilder.OracleIncrementalSource.<RowData>builder()
                        .hostname(ORACLE_CONTAINER.getHost())
                        .port(ORACLE_CONTAINER.getOraclePort())
                        .username(CONNECTOR_USER)
                        .password(CONNECTOR_PWD)
                        .databaseList(ORACLE_DATABASE)
                        .schemaList(ORACLE_SCHEMA)
                        .tableList("DEBEZIUM.CUSTOMERS")
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .startupOptions(StartupOptions.initial())
                        .deserializer(customerTable.getDeserializer())
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
                    // database update operations use TEST_USER rather than CONNECTOR_USER
                    JdbcConfiguration configuration =
                            JdbcConfiguration.copy(
                                            ((JdbcSourceConfig) sourceConfig)
                                                    .getDbzConnectorConfig()
                                                    .getJdbcConfig())
                                    .withUser("debezium")
                                    .withPassword("dbz")
                                    .build();
                    try (OracleConnection oracleConnection =
                            OracleConnectionUtils.createOracleConnection(configuration)) {
                        oracleConnection.execute(statements);
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

    private void testOracleParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testOracleParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testOracleParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testOracleParallelSource(
                parallelism, failoverType, failoverPhase, captureCustomerTables, false);
    }

    private void testOracleParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            boolean skipSnapshotBackfill)
            throws Exception {
        createAndInitialize("customer.sql");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        String sourceDDL =
                format(
                        "CREATE TABLE products ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " ADDRESS STRING,"
                                + " PHONE_NUMBER STRING,"
                                + " primary key (ID) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'false',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        ORACLE_CONTAINER.getUsername(),
                        ORACLE_CONTAINER.getPassword(),
                        ORACLE_DATABASE,
                        ORACLE_SCHEMA,
                        getTableNameRegex(captureCustomerTables), // (customer|customer_1)
                        skipSnapshotBackfill);

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
        TableResult tableResult = tEnv.executeSql("select * from products");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        LOG.info("snapshot data start");
        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the redo log data
        for (String tableId : captureCustomerTables) {
            makeFirstPartRedoLogEvents(ORACLE_SCHEMA + '.' + tableId);
        }
        if (failoverPhase == FailoverPhase.REDO_LOG) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartRedoLogEvents(ORACLE_SCHEMA + '.' + tableId);
        }

        String[] redoLogForSingleTable =
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
        List<String> expectedRedoLogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedRedoLogData.addAll(Arrays.asList(redoLogForSingleTable));
        }
        assertEqualsInAnyOrder(
                expectedRedoLogData, fetchRows(iterator, expectedRedoLogData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private void makeFirstPartRedoLogEvents(String tableId) throws Exception {
        executeSql("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103");
        executeSql("DELETE FROM " + tableId + " where id = 102");
        executeSql("INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')");
        executeSql("UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
    }

    private void makeSecondPartRedoLogEvents(String tableId) throws Exception {
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

    private static List<String> fetchRowData(
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
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private void executeSql(String sql) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    /** The type of failover. */
    private enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    private enum FailoverPhase {
        SNAPSHOT,
        REDO_LOG,
        NEVER
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }
}
