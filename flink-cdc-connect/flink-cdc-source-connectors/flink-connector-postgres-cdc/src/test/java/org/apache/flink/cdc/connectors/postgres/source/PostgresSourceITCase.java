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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.TestTable;
import org.apache.flink.cdc.connectors.postgres.testutils.TestTableId;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.core.execution.JobClient;
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
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils.hasNextData;
import static org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils.triggerFailover;
import static org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils.waitUntilJobRunning;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link PostgresSourceBuilder.PostgresIncrementalSource}. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class PostgresSourceITCase extends PostgresTestBase {

    private static final String DEFAULT_SCAN_STARTUP_MODE = "initial";

    protected static final int DEFAULT_PARALLELISM = 4;

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;
    private static final int USE_POST_HIGHWATERMARK_HOOK = 3;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

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

    @BeforeEach
    public void before() {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testReadSingleTableWithSingleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testReadSingleTableWithMultipleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                4,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testReadMultipleTableWithSingleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testReadMultipleTableWithMultipleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                4,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    // Failover tests
    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testTaskManagerFailoverInSnapshotPhase(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testTaskManagerFailoverInStreamPhase(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.STREAM,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testJobManagerFailoverInSnapshotPhase(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testJobManagerFailoverInStreamPhase(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.STREAM,
                new String[] {"Customers", "customers_1"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testTaskManagerFailoverSingleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                1,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testJobManagerFailoverSingleParallelism(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                1,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers"},
                scanStartupMode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testConsumingTableWithoutPrimaryKey(String scanStartupMode) throws Exception {
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            Assertions.assertThatThrownBy(
                            () -> {
                                testPostgresParallelSource(
                                        1,
                                        scanStartupMode,
                                        PostgresTestUtils.FailoverType.NONE,
                                        PostgresTestUtils.FailoverPhase.NEVER,
                                        new String[] {"customers_no_pk"},
                                        RestartStrategies.noRestart());
                            })
                    .hasStackTraceContaining(
                            "To use incremental snapshot, 'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.");
        } else {
            testPostgresParallelSource(
                    1,
                    scanStartupMode,
                    PostgresTestUtils.FailoverType.NONE,
                    PostgresTestUtils.FailoverPhase.NEVER,
                    new String[] {"customers_no_pk"},
                    RestartStrategies.noRestart());
        }
    }

    @Test
    void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testPostgresParallelSource(
                DEFAULT_PARALLELISM,
                DEFAULT_SCAN_STARTUP_MODE,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                Collections.singletonMap("scan.incremental.snapshot.backfill.skip", "true"));
    }

    @Test
    void testReadSingleTableWithSingleParallelismAndUnboundedChunkFirst() throws Exception {
        testPostgresParallelSource(
                DEFAULT_PARALLELISM,
                DEFAULT_SCAN_STARTUP_MODE,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled", "true"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testDebeziumSlotDropOnStop(String scanStartupMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("debezium.snapshot.fetch.size", "2");
        options.put("debezium.max.batch.size", "3");
        testPostgresParallelSource(
                2,
                scanStartupMode,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.STREAM,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                options,
                this::checkStreamDataWithDDLDuringFailover);
    }

    @Test
    void testReadSingleTableMutilpleFetch() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("debezium.snapshot.fetch.size", "2");
        options.put("debezium.max.batch.size", "3");
        testPostgresParallelSource(
                1,
                DEFAULT_SCAN_STARTUP_MODE,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                options);
    }

    @Test
    void testSnapshotOnlyModeWithDMLPostHighWaterMark() throws Exception {
        // The data num is 21, set fetchSize = 22 to test the job is bounded.
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 22, USE_POST_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
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
                        "+I[2000, user_21, Shanghai, 123567891234]");
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSnapshotOnlyModeWithDMLPreHighWaterMark() throws Exception {
        // The data num is 21, set fetchSize = 22 to test the job is bounded
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 22, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
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
        // when enable backfill, the wal log between (snapshot, high_watermark) will be
        // applied as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testEnableBackfillWithDMLPreHighWaterMark(String scanStartupMode) throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.initial());

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

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testEnableBackfillWithDMLPostLowWaterMark(String scanStartupMode) throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_POST_LOWWATERMARK_HOOK, StartupOptions.initial());

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

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testSkipBackfillWithDMLPreHighWaterMark(String scanStartupMode) throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records =
                testBackfillWhenWritingEvents(
                        true, 25, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.initial());

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

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testSkipBackfillWithDMLPostLowWaterMark(String scanStartupMode) throws Exception {
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            return;
        }

        List<String> records =
                testBackfillWhenWritingEvents(
                        true, 25, USE_POST_LOWWATERMARK_HOOK, StartupOptions.initial());

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

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testNewLsnCommittedWhenCheckpoint(String scanStartupMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("scan.incremental.snapshot.backfill.skip", "false");
        options.put("connector", "postgres-cdc-mock");
        testPostgresParallelSource(
                1,
                scanStartupMode,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.STREAM,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                options,
                this::checkStreamDataWithHook);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    public void testTableWithChunkColumnOfNoPrimaryKey(String scanStartupMode) throws Exception {
        Assumptions.assumeThat(scanStartupMode).isEqualTo(DEFAULT_SCAN_STARTUP_MODE);
        String chunkColumn = "Name";
        testPostgresParallelSource(
                1,
                scanStartupMode,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                RestartStrategies.noRestart(),
                Collections.singletonMap(
                        "scan.incremental.snapshot.chunk.key-column", chunkColumn));

        // since `scan.incremental.snapshot.chunk.key-column` is set, an exception should not occur.
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testHeartBeat(String scanStartupMode) throws Exception {
        try (PostgresConnection connection = getConnection()) {
            connection.execute("CREATE TABLE IF NOT EXISTS heart_beat_table(a int)");
            connection.commit();
        }

        TableId tableId = new TableId(null, "public", "heart_beat_table");
        try (PostgresConnection connection = getConnection()) {
            Assertions.assertThat(getCountOfTable(connection, tableId)).isZero();
        }

        Map<String, String> options = new HashMap<>();
        options.put("heartbeat.interval.ms", "100");
        options.put("debezium.heartbeat.action.query", "INSERT INTO heart_beat_table VALUES(1)");
        testPostgresParallelSource(
                1,
                scanStartupMode,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                RestartStrategies.noRestart(),
                options);
        try (PostgresConnection connection = getConnection()) {
            assertThat(getCountOfTable(connection, tableId)).isGreaterThan(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testCommitLsnWhenTaskManagerFailover(String scanStartupMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("scan.incremental.snapshot.backfill.skip", "false");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("scan.lsn-commit.checkpoints-num-delay", "0");
        testPostgresParallelSource(
                1,
                scanStartupMode,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.STREAM,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                options,
                this::checkStreamDataWithTestLsn);
    }

    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testAppendOnly(String scanStartupMode) throws Exception {
        testPostgresParallelSource(
                1,
                scanStartupMode,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                new String[] {"Customers"},
                RestartStrategies.fixedDelayRestart(1, 0),
                Collections.singletonMap("scan.read-changelog-as-append-only.enabled", "true"),
                this::checkStreamDataAsAppend);
    }

    private List<String> testBackfillWhenWritingEvents(
            boolean skipSnapshotBackfill,
            int fetchSize,
            int hookType,
            StartupOptions startupOptions)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setParallelism(1);

        ResolvedSchema customersSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("Id", BIGINT().notNull()),
                                physical("Name", STRING()),
                                physical("address", STRING()),
                                physical("phone_number", STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        TestTableId tableId = new TestTableId("customer", "Customers");
        TestTable table = new TestTable(customersSchema);

        PostgresSourceBuilder.PostgresIncrementalSource<RowData> source =
                PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                        .hostname(customDatabase.getHost())
                        .port(customDatabase.getDatabasePort())
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .database(customDatabase.getDatabaseName())
                        .decodingPluginName("pgoutput")
                        .slotName(slotName)
                        .tableList(tableId.toString())
                        .startupOptions(startupOptions)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .lsnCommitCheckpointsDelay(1)
                        .deserializer(table.getDeserializer())
                        .build();

        // Do some database operations during hook in snapshot period.
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        String[] statements =
                new String[] {
                    String.format(
                            "INSERT INTO %s VALUES (15213, 'user_15213', 'Shanghai', '123567891234')",
                            tableId.toSql()),
                    String.format(
                            "UPDATE %s SET address = 'Pittsburgh' WHERE \"Id\" = 2000",
                            tableId.toSql()),
                    String.format("DELETE FROM %s WHERE \"Id\" = 1019", tableId.toSql())
                };
        SnapshotPhaseHook snapshotPhaseHook =
                (sourceConfig, split) -> {
                    try (PostgresDialect dialect =
                                    new PostgresDialect((PostgresSourceConfig) sourceConfig);
                            PostgresConnection postgresConnection = dialect.openJdbcConnection()) {
                        postgresConnection.execute(statements);
                        postgresConnection.commit();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        switch (hookType) {
            case USE_POST_LOWWATERMARK_HOOK:
                hooks.setPostLowWatermarkAction(snapshotPhaseHook);
                break;
            case USE_PRE_HIGHWATERMARK_HOOK:
                hooks.setPreHighWatermarkAction(snapshotPhaseHook);
                break;
            case USE_POST_HIGHWATERMARK_HOOK:
                hooks.setPostHighWatermarkAction(snapshotPhaseHook);
                break;
        }
        source.setSnapshotHooks(hooks);

        List<String> records;
        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Backfill Skipped Source")
                        .executeAndCollect()) {
            records = fetchRowData(iterator, fetchSize, table::stringify);
            env.close();
        }
        return records;
    }

    private void testPostgresParallelSource(
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            String scanStartupMode)
            throws Exception {
        testPostgresParallelSource(
                DEFAULT_PARALLELISM,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                scanStartupMode);
    }

    private void testPostgresParallelSource(
            int parallelism,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            String scanStartupMode)
            throws Exception {
        testPostgresParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                RestartStrategies.fixedDelayRestart(1, 0));
    }

    private void testPostgresParallelSource(
            int parallelism,
            String scanStartupMode,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration)
            throws Exception {
        testPostgresParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                restartStrategyConfiguration,
                new HashMap<>());
    }

    private void testPostgresParallelSource(
            int parallelism,
            String scanStartupMode,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
            Map<String, String> otherOptions)
            throws Exception {
        testPostgresParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                restartStrategyConfiguration,
                otherOptions,
                this::checkStreamData);
    }

    private void testPostgresParallelSource(
            int parallelism,
            String scanStartupMode,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
            Map<String, String> otherOptions,
            StreamDataChecker streamDataChecker)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(restartStrategyConfiguration);

        tEnv.executeSql(getSourceDDL(scanStartupMode, captureCustomerTables, otherOptions));
        TableResult tableResult = tEnv.executeSql("select * from customers");

        // first step: check the snapshot data
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(tableResult, failoverType, failoverPhase, captureCustomerTables);
        }

        // second step: check the stream data
        streamDataChecker.check(tableResult, failoverType, failoverPhase, captureCustomerTables);

        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        optionalJobClient.get().cancel().get();

        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
    }

    private String getSourceDDL(
            String scanStartupMode,
            String[] captureCustomerTables,
            Map<String, String> otherOptions) {
        return format(
                "CREATE TABLE customers ("
                        + " Id BIGINT NOT NULL,"
                        + " Name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " primary key (Id) not enforced"
                        + ") WITH ("
                        + " 'connector' = '%s',"
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
                        + " 'decoding.plugin.name' = 'pgoutput', "
                        + " 'slot.name' = '%s',"
                        + " 'scan.lsn-commit.checkpoints-num-delay' = '1'"
                        + " %s"
                        + ")",
                otherOptions.getOrDefault("connector", "postgres-cdc"),
                customDatabase.getHost(),
                customDatabase.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                SCHEMA_NAME,
                getTableNameRegex(captureCustomerTables),
                scanStartupMode,
                slotName,
                otherOptions.isEmpty()
                        ? ""
                        : ","
                                + otherOptions.entrySet().stream()
                                        .map(
                                                e ->
                                                        String.format(
                                                                "'%s'='%s'",
                                                                e.getKey(), e.getValue()))
                                        .collect(Collectors.joining(",")));
    }

    private void checkSnapshotData(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
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
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == PostgresTestUtils.FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(3000));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
    }

    private void checkStreamData(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        for (String tableName : captureCustomerTables) {
            makeFirstPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        // wait for the stream reading
        Thread.sleep(2000L);

        if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableName : captureCustomerTables) {
            makeSecondPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        List<String> expectedStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedStreamData.addAll(firstPartStreamEvents);
            expectedStreamData.addAll(secondPartStreamEvents);
        }
        // wait for the stream reading
        Thread.sleep(2000L);

        assertEqualsInAnyOrder(expectedStreamData, fetchRows(iterator, expectedStreamData.size()));
        Assertions.assertThat(hasNextData(iterator)).isFalse();
    }

    private void checkStreamDataAsAppend(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        checkStreamData(tableResult, failoverType, failoverPhase, captureCustomerTables, true);
    }

    private void checkStreamData(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            boolean appendOnly)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        for (String tableName : captureCustomerTables) {
            makeFirstPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        // wait for the stream reading
        Thread.sleep(2000L);

        if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableName : captureCustomerTables) {
            makeSecondPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        List<String> expectedStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedStreamData.addAll(
                    appendOnly
                            ? firstPartStreamEvents.stream()
                                    .map(op -> op.replaceAll("(-U)|(\\+U)|(-D)", "+I"))
                                    .collect(Collectors.toList())
                            : firstPartStreamEvents);
            expectedStreamData.addAll(
                    appendOnly
                            ? secondPartStreamEvents.stream()
                                    .map(op -> op.replaceAll("(-U)|(\\+U)|(-D)", "+I"))
                                    .collect(Collectors.toList())
                            : secondPartStreamEvents);
        }
        // wait for the stream reading
        Thread.sleep(2000L);

        assertEqualsInAnyOrder(expectedStreamData, fetchRows(iterator, expectedStreamData.size()));
        Assertions.assertThat(hasNextData(iterator)).isFalse();
    }

    private void checkStreamDataWithHook(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        final AtomicLong savedCheckpointId = new AtomicLong(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                checkpointId -> {
                    try {
                        if (savedCheckpointId.get() == 0) {
                            savedCheckpointId.set(checkpointId);

                            for (String tableName : captureCustomerTables) {
                                makeFirstPartStreamEvents(
                                        getConnection(), new TestTableId(SCHEMA_NAME, tableName));
                            }
                            // wait for the stream reading
                            Thread.sleep(2000L);

                            triggerFailover(
                                    failoverType,
                                    jobId,
                                    miniClusterResource.get().getMiniCluster(),
                                    () -> sleepMs(200));
                            countDownLatch.countDown();
                        }
                    } catch (Exception e) {
                        throw new FlinkRuntimeException(e);
                    }
                });

        countDownLatch.await();
        waitUntilJobRunning(tableResult);

        if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableName : captureCustomerTables) {
            makeSecondPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        List<String> expectedStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedStreamData.addAll(firstPartStreamEvents);
            expectedStreamData.addAll(secondPartStreamEvents);
        }
        // wait for the stream reading
        Thread.sleep(2000L);

        assertEqualsInAnyOrder(expectedStreamData, fetchRows(iterator, expectedStreamData.size()));
        Assertions.assertThat(hasNextData(iterator)).isFalse();
    }

    private void checkStreamDataWithDDLDuringFailover(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        for (String tableName : captureCustomerTables) {
            makeFirstPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        // wait for the stream reading
        Thread.sleep(2000L);

        // update database during stream fail over period
        if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> {
                        for (String tableName : captureCustomerTables) {
                            try {
                                makeSecondPartStreamEvents(
                                        getConnection(), new TestTableId(SCHEMA_NAME, tableName));
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
        Assertions.assertThat(hasNextData(iterator)).isFalse();
    }

    private void checkStreamDataWithTestLsn(
            TableResult tableResult,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        assertThat(optionalJobClient).isPresent();
        JobID jobId = optionalJobClient.get().getJobID();

        for (String tableName : captureCustomerTables) {
            makeFirstPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }

        // wait for the stream reading and isCommitOffset is true
        Thread.sleep(20000L);

        String confirmedFlushLsn;
        try (PostgresConnection connection = getConnection()) {
            confirmedFlushLsn = getConfirmedFlushLsn(connection);
        }
        if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        // wait for the stream reading and isCommitOffset is true
        Thread.sleep(30000L);
        for (String tableName : captureCustomerTables) {
            makeSecondPartStreamEvents(getConnection(), new TestTableId(SCHEMA_NAME, tableName));
        }
        Thread.sleep(5000L);
        try (PostgresConnection connection = getConnection()) {
            Assertions.assertThat(getConfirmedFlushLsn(connection)).isNotEqualTo(confirmedFlushLsn);
        }
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

    public static List<String> fetchRows(Iterator<Row> iter, int size) {
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
    private void makeFirstPartStreamEvents(JdbcConnection connection, TestTableId tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make stream events for the first split
            connection.execute(
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2', 'Shanghai', '123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    /**
     * Make some other changes on the specified customer table. Changelog in string could be
     * accessed by {@link #secondPartStreamEvents}.
     */
    private void makeSecondPartStreamEvents(JdbcConnection connection, TestTableId tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make stream events for split-1
            connection.execute(
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 1010");
            connection.commit();

            // make stream events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId.toSql()
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

    private static long getCountOfTable(JdbcConnection jdbc, TableId tableId) throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        // https://stackoverflow.com/questions/7943233/fast-way-to-discover-the-row-count-of-a-table-in-postgresql
        // NOTE: it requires ANALYZE or VACUUM to be run first in PostgreSQL.
        final String query = String.format("SELECT COUNT(1) FROM %s", tableId.toString());

        return jdbc.queryAndMap(
                query,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getLong(1);
                });
    }

    private String getConfirmedFlushLsn(JdbcConnection jdbc) throws SQLException {
        final String query =
                String.format(
                        "SELECT\n"
                                + "confirmed_flush_lsn\n"
                                + "FROM pg_replication_slots where slot_name = '%s'",
                        slotName);

        return jdbc.queryAndMap(
                query,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getString(1);
                });
    }

    private interface StreamDataChecker {
        void check(
                TableResult tableResult,
                PostgresTestUtils.FailoverType failoverType,
                PostgresTestUtils.FailoverPhase failoverPhase,
                String[] captureCustomerTables)
                throws Exception;
    }
}
