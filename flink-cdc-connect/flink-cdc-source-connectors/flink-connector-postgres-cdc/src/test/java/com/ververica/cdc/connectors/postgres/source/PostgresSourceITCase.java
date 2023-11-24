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

package com.ververica.cdc.connectors.postgres.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import com.ververica.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import com.ververica.cdc.connectors.postgres.PostgresTestBase;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.testutils.TestTable;
import com.ververica.cdc.connectors.postgres.testutils.UniqueDatabase;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/** IT tests for {@link PostgresSourceBuilder.PostgresIncrementalSource}. */
@RunWith(Parameterized.class)
public class PostgresSourceITCase extends PostgresTestBase {

    private static final String DEFAULT_SCAN_STARTUP_MODE = "initial";

    protected static final int DEFAULT_PARALLELISM = 4;

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private final String scanStartupMode;

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    /** First part stream events, which is made by {@link #makeFirstPartStreamEvents}. */
    private final List<String> firstPartStreamEvents =
            Arrays.asList(
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]");

    /** Second part stream events, which is made by {@link #makeSecondPartStreamEvents}. */
    private final List<String> secondPartStreamEvents =
            Arrays.asList(
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]");

    public PostgresSourceITCase(String scanStartupMode) {
        this.scanStartupMode = scanStartupMode;
    }

    @Parameterized.Parameters(name = "scanStartupMode: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {"initial"}, new Object[] {"latest-offset"}};
    }

    @Before
    public void before() {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @After
    public void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testPostgresParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testPostgresParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testPostgresParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testPostgresParallelSource(
                4,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testPostgresParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverInStreamPhase() throws Exception {
        testPostgresParallelSource(
                FailoverType.TM, FailoverPhase.STREAM, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testPostgresParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInStreamPhase() throws Exception {
        testPostgresParallelSource(
                FailoverType.JM, FailoverPhase.STREAM, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testPostgresParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testPostgresParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testConsumingTableWithoutPrimaryKey() throws Exception {
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            try {
                testPostgresParallelSource(
                        1,
                        scanStartupMode,
                        FailoverType.NONE,
                        FailoverPhase.NEVER,
                        new String[] {"customers_no_pk"},
                        RestartStrategies.noRestart(),
                        false);
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowableWithMessage(
                                        e,
                                        String.format(
                                                "Incremental snapshot for tables requires primary key, but table %s doesn't have primary key",
                                                SCHEMA_NAME + ".customers_no_pk"))
                                .isPresent());
            }
        } else {
            testPostgresParallelSource(
                    1,
                    scanStartupMode,
                    FailoverType.NONE,
                    FailoverPhase.NEVER,
                    new String[] {"customers_no_pk"},
                    RestartStrategies.noRestart(),
                    false);
        }
    }

    @Test
    public void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testPostgresParallelSource(
                DEFAULT_PARALLELISM,
                DEFAULT_SCAN_STARTUP_MODE,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                true);
    }

    @Test
    public void testDebeziumSlotDropOnStop() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(2);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'slot.name' = '%s', "
                                + " 'debezium.slot.drop.on.stop' = 'true'"
                                + ")",
                        customDatabase.getHost(),
                        customDatabase.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        SCHEMA_NAME,
                        "customers",
                        scanStartupMode,
                        slotName);
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");

        // first step: check the snapshot data
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(
                    tableResult, FailoverType.JM, FailoverPhase.STREAM, new String[] {"customers"});
        }

        // second step: check the stream data
        checkStreamDataWithDDLDuringFailover(
                tableResult, FailoverType.JM, FailoverPhase.STREAM, new String[] {"customers"});

        tableResult.getJobClient().get().cancel().get();
    }

    @Test
    public void testEnableBackfillWithDMLPreHighWaterMark() throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records = testBackfillWhenWritingEvents(false, 21, USE_PRE_HIGHWATERMARK_HOOK);

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
        // when enable backfill, the wal log between (snapshot,  high_watermark) will be
        // applied as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testEnableBackfillWithDMLPostLowWaterMark() throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records = testBackfillWhenWritingEvents(false, 21, USE_POST_LOWWATERMARK_HOOK);

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
        // when enable backfill, the wal log between (low_watermark, snapshot) will be applied
        // as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testSkipBackfillWithDMLPreHighWaterMark() throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

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
        // when skip backfill, the wal log between (snapshot,  high_watermark) will be seen as
        // stream event.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testSkipBackfillWithDMLPostLowWaterMark() throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

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
        // when skip backfill, the wal log between (snapshot,  high_watermark) will still be
        // seen as stream event. This will occur data duplicate. For example, user_20 will be
        // deleted twice, and user_15213 will be inserted twice.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    private List<String> testBackfillWhenWritingEvents(
            boolean skipSnapshotBackfill, int fetchSize, int hookType) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setParallelism(1);

        ResolvedSchema customersSchame =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("id", BIGINT().notNull()),
                                physical("name", STRING()),
                                physical("address", STRING()),
                                physical("phone_number", STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        TestTable customerTable =
                new TestTable(customDatabase, "customer", "customers", customersSchame);
        String tableId = customerTable.getTableId();

        PostgresSourceBuilder.PostgresIncrementalSource source =
                PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                        .hostname(customDatabase.getHost())
                        .port(customDatabase.getDatabasePort())
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .database(customDatabase.getDatabaseName())
                        .slotName(slotName)
                        .tableList(tableId)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
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
                    PostgresDialect dialect =
                            new PostgresDialect((PostgresSourceConfig) sourceConfig);
                    try (PostgresConnection postgresConnection = dialect.openJdbcConnection()) {
                        postgresConnection.execute(statements);
                        postgresConnection.commit();
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

        List<String> records;
        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Backfill Skipped Source")
                        .executeAndCollect()) {
            records = fetchRowData(iterator, fetchSize, customerTable::stringify);
            env.close();
        }
        return records;
    }

    private void testPostgresParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testPostgresParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testPostgresParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testPostgresParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                RestartStrategies.fixedDelayRestart(1, 0),
                false);
    }

    private void testPostgresParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
            boolean skipSnapshotBackfill)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(restartStrategyConfiguration);
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + ")",
                        customDatabase.getHost(),
                        customDatabase.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        SCHEMA_NAME,
                        getTableNameRegex(captureCustomerTables),
                        scanStartupMode,
                        slotName,
                        skipSnapshotBackfill);
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");

        // first step: check the snapshot data
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(tableResult, failoverType, failoverPhase, captureCustomerTables);
        }

        // second step: check the stream data
        checkStreamData(tableResult, failoverType, failoverPhase, captureCustomerTables);

        tableResult.getJobClient().get().cancel().get();

        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
    }

    private void checkSnapshotData(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
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

        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(3000));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
    }

    private void checkStreamData(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        for (String tableId : captureCustomerTables) {
            makeFirstPartStreamEvents(
                    getConnection(),
                    customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableId);
        }

        // wait for the stream reading
        Thread.sleep(2000L);

        if (failoverPhase == FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartStreamEvents(
                    getConnection(),
                    customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableId);
        }

        List<String> expectedStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedStreamData.addAll(firstPartStreamEvents);
            expectedStreamData.addAll(secondPartStreamEvents);
        }
        // wait for the stream reading
        Thread.sleep(2000L);

        assertEqualsInAnyOrder(expectedStreamData, fetchRows(iterator, expectedStreamData.size()));
        assertTrue(!hasNextData(iterator));
    }

    private void checkStreamDataWithDDLDuringFailover(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        for (String tableId : captureCustomerTables) {
            makeFirstPartStreamEvents(
                    getConnection(),
                    customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableId);
        }

        // wait for the stream reading
        Thread.sleep(2000L);

        // update database during stream fail over period
        if (failoverPhase == FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.getMiniCluster(),
                    () -> {
                        for (String tableId : captureCustomerTables) {
                            try {
                                makeSecondPartStreamEvents(
                                        getConnection(),
                                        customDatabase.getDatabaseName()
                                                + '.'
                                                + SCHEMA_NAME
                                                + '.'
                                                + tableId);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        sleepMs(200);
                    });
            waitUntilJobRunning(tableResult);
        }

        List<String> expectedStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedStreamData.addAll(firstPartStreamEvents);
            expectedStreamData.addAll(secondPartStreamEvents);
        }
        // wait for the stream reading
        Thread.sleep(2000L);

        assertEqualsInAnyOrder(expectedStreamData, fetchRows(iterator, expectedStreamData.size()));
        assertTrue(!hasNextData(iterator));
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
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

    /**
     * Make some changes on the specified customer table. Changelog in string could be accessed by
     * {@link #firstPartStreamEvents}.
     */
    private void makeFirstPartStreamEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make stream events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    /**
     * Make some other changes on the specified customer table. Changelog in string could be
     * accessed by {@link #secondPartStreamEvents}.
     */
    private void makeSecondPartStreamEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make stream events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make stream events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                            + " (2002, 'user_23','Shanghai','123567891234'),"
                            + "(2003, 'user_24','Shanghai','123567891234')");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private PostgresConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", customDatabase.getHost());
        properties.put("port", String.valueOf(customDatabase.getDatabasePort()));
        properties.put("user", customDatabase.getUsername());
        properties.put("password", customDatabase.getPassword());
        properties.put("dbname", customDatabase.getDatabaseName());
        return createConnection(properties);
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
        STREAM,
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

    private void waitUntilJobRunning(TableResult tableResult)
            throws InterruptedException, ExecutionException {
        do {
            Thread.sleep(5000L);
        } while (tableResult.getJobClient().get().getJobStatus().get() != RUNNING);
    }

    private boolean hasNextData(final CloseableIterator<?> iterator)
            throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            FutureTask<Boolean> future = new FutureTask(iterator::hasNext);
            executor.execute(future);
            return future.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return false;
        } finally {
            executor.shutdown();
        }
    }
}
