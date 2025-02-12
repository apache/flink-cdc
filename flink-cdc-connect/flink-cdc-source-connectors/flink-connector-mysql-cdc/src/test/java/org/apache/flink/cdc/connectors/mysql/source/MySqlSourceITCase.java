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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTable;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTableSchemas;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
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
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** IT tests for {@link MySqlSource}. */
@RunWith(Parameterized.class)
public class MySqlSourceITCase extends MySqlSourceTestBase {

    public static final Duration TIMEOUT = Duration.ofSeconds(300);

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(TIMEOUT.getSeconds());

    private static final String DEFAULT_SCAN_STARTUP_MODE = "initial";
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    /** Initial changelogs in string of table "customers" in database "customer". */
    private final List<String> initialChanges =
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

    /** First part binlog events in string, which is made by {@link #makeFirstPartBinlogEvents}. */
    private final List<String> firstPartBinlogEvents =
            Arrays.asList(
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]");

    /**
     * Second part binlog events in string, which is made by {@link #makeSecondPartBinlogEvents}.
     */
    private final List<String> secondPartBinlogEvents =
            Arrays.asList(
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]");

    @Parameterized.Parameter public String tableName;

    @Parameterized.Parameter(1)
    public String chunkColumnName;

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private static final int USE_POST_HIGHWATERMARK_HOOK = 3;

    @Parameterized.Parameters(name = "table: {0}, chunkColumn: {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {"customers", null}, {"customers", "id"}, {"customers_no_pk", "id"}
                });
    }

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {tableName});
    }

    @Test
    public void testReadSingleTableWithSingleParallelismAndSkipBackFill() throws Exception {
        testMySqlParallelSource(
                1,
                DEFAULT_SCAN_STARTUP_MODE,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {tableName},
                RestartStrategies.fixedDelayRestart(1, 0),
                true);
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testMySqlParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {tableName});
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testMySqlParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {tableName, "customers_1"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverInBinlogPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.TM, FailoverPhase.BINLOG, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverFromLatestOffset() throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM,
                "latest-offset",
                FailoverType.TM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                RestartStrategies.fixedDelayRestart(1, 0));
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInBinlogPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.JM, FailoverPhase.BINLOG, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testJobManagerFailoverFromLatestOffset() throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM,
                "latest-offset",
                FailoverType.JM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                RestartStrategies.fixedDelayRestart(1, 0));
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {tableName});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {tableName});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSnapshotSplitReadingFailCrossCheckpoints() throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        // The sleeping source will sleep awhile after send per record
        MySqlSource<RowData> sleepingSource = buildSleepingSource();
        DataStreamSource<RowData> source =
                env.fromSource(sleepingSource, WatermarkStrategy.noWatermarks(), "selfSource");

        String[] expectedSnapshotData =
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
        TypeSerializer<RowData> serializer =
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<RowData> factory =
                new CollectSinkOperatorFactory(serializer, accumulatorName);
        CollectSinkOperator<RowData> operator = (CollectSinkOperator) factory.getOperator();
        CollectResultIterator<RowData> iterator =
                new CollectResultIterator(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig(),
                        10000L);
        CollectStreamSink<RowData> sink = new CollectStreamSink(source, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        JobClient jobClient = env.executeAsync("snapshotSplitTest");
        iterator.setJobClient(jobClient);
        JobID jobId = jobClient.getJobID();

        // Trigger failover once some snapshot records has been sent by sleeping source
        if (iterator.hasNext()) {
            triggerFailover(
                    FailoverType.JM,
                    jobId,
                    miniClusterResource.getMiniCluster(),
                    () -> sleepMs(100));
        }

        // Check all snapshot records are sent with exactly-once semantics
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshotData),
                fetchRowData(iterator, expectedSnapshotData.length));
        assertTrue(!hasNextData(iterator));
        jobClient.cancel().get();
    }

    @Test
    public void testStartFromEarliestOffset() throws Exception {
        List<String> expected = new ArrayList<>();
        expected.addAll(initialChanges);
        expected.addAll(firstPartBinlogEvents);
        testStartingOffset(StartupOptions.earliest(), expected);
    }

    @Test
    public void testStartFromLatestOffset() throws Exception {
        testStartingOffset(StartupOptions.latest(), Collections.emptyList());
    }

    @Test
    public void testSnapshotOnlyModeWithDMLPostHighWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_POST_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
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
    public void testSnapshotOnlyModeWithDMLPreHighWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
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

    @Test
    public void testEnableBackfillWithDMLPreHighWaterMark() throws Exception {

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
        // when enable backfill, the wal log between (snapshot, high_watermark) will be
        // applied as snapshot image
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testEnableBackfillWithDMLPostLowWaterMark() throws Exception {

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

    @Test
    public void testSkipBackfillWithDMLPreHighWaterMark() throws Exception {

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
        // when skip backfill, the wal log between (snapshot, high_watermark) will be seen as
        // stream event.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    public void testSkipBackfillWithDMLPostLowWaterMark() throws Exception {

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
        // when skip backfill, the wal log between (snapshot, high_watermark) will still be
        // seen as stream event. This will occur data duplicate. For example, user_20 will be
        // deleted twice, and user_15213 will be inserted twice.
        assertEqualsInAnyOrder(expectedRecords, records);
    }

    private List<String> testBackfillWhenWritingEvents(
            boolean skipSnapshotBackfill,
            int fetchSize,
            int hookType,
            StartupOptions startupOptions)
            throws Exception {
        customDatabase.createAndInitialize();
        TestTable customerTable =
                new TestTable(customDatabase, "customers", TestTableSchemas.CUSTOMERS);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<RowData> source =
                MySqlSource.<RowData>builder()
                        .hostname(customDatabase.getHost())
                        .port(customDatabase.getDatabasePort())
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .serverTimeZone("UTC")
                        .databaseList(customDatabase.getDatabaseName())
                        .tableList(customerTable.getTableId())
                        .deserializer(customerTable.getDeserializer())
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .startupOptions(startupOptions)
                        .build();

        String[] statements =
                new String[] {
                    String.format(
                            "INSERT INTO %s VALUES (15213, 'user_15213', 'Shanghai', '123567891234')",
                            customerTable.getTableId()),
                    String.format(
                            "UPDATE %s SET address='Pittsburgh' WHERE id=2000",
                            customerTable.getTableId()),
                    String.format("DELETE FROM %s WHERE id=1019", customerTable.getTableId())
                };

        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        SnapshotPhaseHook snapshotPhaseHook =
                (connection, split) -> {
                    connection.setAutoCommit(false);
                    connection.execute(statements);
                    connection.commit();
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
        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Backfill Skipped Source")
                        .executeAndCollect()) {
            List<String> records = fetchRowData(iterator, fetchSize, customerTable::stringify);
            return records;
        }
    }

    private void testStartingOffset(
            StartupOptions startupOptions, List<String> expectedChangelogAfterStart)
            throws Exception {
        // Initialize customer database
        customDatabase.createAndInitialize();
        String tableId = getTableId();

        // Make some changes before starting the CDC job
        makeFirstPartBinlogEvents(getConnection(), tableId);

        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build deserializer
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);
        RowDataDebeziumDeserializeSchema deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType((RowType) dataType.getLogicalType())
                        .setResultTypeInfo(typeInfo)
                        .build();

        // Build source
        MySqlSource<RowData> mySqlSource =
                MySqlSource.<RowData>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(customDatabase.getDatabaseName())
                        .serverTimeZone("UTC")
                        .tableList(tableId)
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(deserializer)
                        .startupOptions(startupOptions)
                        .chunkKeyColumn(
                                new ObjectPath(customDatabase.getDatabaseName(), tableName),
                                chunkColumnName)
                        .build();

        // Build and execute the job
        DataStreamSource<RowData> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        try (CloseableIterator<RowData> iterator = source.executeAndCollect()) {
            List<String> rows = fetchRowData(iterator, expectedChangelogAfterStart.size());
            assertEqualsInAnyOrder(expectedChangelogAfterStart, rows);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceMetrics() throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> source =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(customDatabase.getDatabaseName())
                        .tableList(customDatabase.getDatabaseName() + ".customers")
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .serverId(getServerId())
                        .serverTimeZone("UTC")
                        .build();
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        CollectResultIterator<String> iterator = addCollector(env, stream);
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);

        // ---------------------------- Snapshot phase ------------------------------
        // Wait until we receive all 21 snapshot records
        int numSnapshotRecordsExpected = 21;
        int numSnapshotRecordsReceived = 0;
        while (numSnapshotRecordsReceived < numSnapshotRecordsExpected && iterator.hasNext()) {
            iterator.next();
            numSnapshotRecordsReceived++;
        }

        // Check metrics
        List<OperatorMetricGroup> metricGroups =
                metricReporter.findOperatorMetricGroups(jobClient.getJobID(), "MySQL CDC Source");
        // There should be only 1 parallelism of source, so it's safe to get the only group
        OperatorMetricGroup group = metricGroups.get(0);
        Map<String, Metric> metrics = metricReporter.getMetricsByGroup(group);

        // numRecordsOut
        assertEquals(
                numSnapshotRecordsExpected,
                group.getIOMetricGroup().getNumRecordsOutCounter().getCount());

        // currentEmitEventTimeLag should be UNDEFINED during snapshot phase
        assertTrue(metrics.containsKey(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG));
        Gauge<Long> currentEmitEventTimeLag =
                (Gauge<Long>) metrics.get(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG);
        assertEquals(
                InternalSourceReaderMetricGroup.UNDEFINED,
                (long) currentEmitEventTimeLag.getValue());

        // currentFetchEventTimeLag should be UNDEFINED during snapshot phase
        assertTrue(metrics.containsKey(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG));
        Gauge<Long> currentFetchEventTimeLag =
                (Gauge<Long>) metrics.get(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG);
        assertEquals(
                MySqlSourceReaderMetrics.UNDEFINED, (long) currentFetchEventTimeLag.getValue());

        // sourceIdleTime should be positive (we can't know the exact value)
        assertTrue(metrics.containsKey(MetricNames.SOURCE_IDLE_TIME));
        Gauge<Long> sourceIdleTime = (Gauge<Long>) metrics.get(MetricNames.SOURCE_IDLE_TIME);
        assertTrue(sourceIdleTime.getValue() > 0);
        assertTrue(sourceIdleTime.getValue() < TIMEOUT.toMillis());

        // --------------------------------- Binlog phase -----------------------------
        makeFirstPartBinlogEvents(getConnection(), customDatabase.qualifiedTableName("customers"));
        // Wait until we receive 4 changes made above
        int numBinlogRecordsExpected = 4;
        int numBinlogRecordsReceived = 0;
        while (numBinlogRecordsReceived < numBinlogRecordsExpected && iterator.hasNext()) {
            iterator.next();
            numBinlogRecordsReceived++;
        }

        // Check metrics
        // numRecordsOut
        assertEquals(
                numSnapshotRecordsExpected + numBinlogRecordsExpected,
                group.getIOMetricGroup().getNumRecordsOutCounter().getCount());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(currentEmitEventTimeLag.getValue() > 0);
        assertTrue(currentEmitEventTimeLag.getValue() < TIMEOUT.toMillis());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(currentFetchEventTimeLag.getValue() > 0);
        assertTrue(currentFetchEventTimeLag.getValue() < TIMEOUT.toMillis());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(sourceIdleTime.getValue() > 0);
        assertTrue(sourceIdleTime.getValue() < TIMEOUT.toMillis());

        jobClient.cancel().get();
        iterator.close();
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env, DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getTransformation().getOutputType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig(),
                        10000L);
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        return iterator;
    }

    private MySqlSource<RowData> buildSleepingSource() {
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.BIGINT().notNull()),
                                Column.physical("name", DataTypes.STRING()),
                                Column.physical("address", DataTypes.STRING()),
                                Column.physical("phone_number", DataTypes.STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = new MetadataConverter[0];
        final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(physicalDataType);

        SleepingRowDataDebeziumDeserializeSchema deserializer =
                new SleepingRowDataDebeziumDeserializeSchema(
                        RowDataDebeziumDeserializeSchema.newBuilder()
                                .setPhysicalRowType(physicalDataType)
                                .setMetadataConverters(metadataConverters)
                                .setResultTypeInfo(typeInfo)
                                .setServerTimeZone(ZoneId.of("UTC"))
                                .setUserDefinedConverterFactory(
                                        MySqlDeserializationConverterFactory.instance())
                                .build(),
                        1000L);
        return MySqlSource.<RowData>builder()
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .databaseList(customDatabase.getDatabaseName())
                .tableList(getTableId())
                .username(customDatabase.getUsername())
                .password(customDatabase.getPassword())
                .serverTimeZone("UTC")
                .serverId(getServerId())
                .splitSize(8096)
                .splitMetaGroupSize(1000)
                .distributionFactorUpper(1000.0d)
                .distributionFactorLower(0.05d)
                .fetchSize(1024)
                .connectTimeout(Duration.ofSeconds(30))
                .connectMaxRetries(3)
                .connectionPoolSize(20)
                .debeziumProperties(new Properties())
                .startupOptions(StartupOptions.initial())
                .deserializer(deserializer)
                .scanNewlyAddedTableEnabled(false)
                .jdbcProperties(new Properties())
                .heartbeatInterval(Duration.ofSeconds(30))
                .chunkKeyColumn(
                        new ObjectPath(customDatabase.getDatabaseName(), tableName),
                        chunkColumnName)
                .build();
    }

    private void testMySqlParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testMySqlParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testMySqlParallelSource(
                parallelism,
                DEFAULT_SCAN_STARTUP_MODE,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                RestartStrategies.fixedDelayRestart(1, 0));
    }

    private void testMySqlParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration)
            throws Exception {
        testMySqlParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                restartStrategyConfiguration,
                false);
    }

    private void testMySqlParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
            boolean skipSnapshotBackfill)
            throws Exception {
        customDatabase.createAndInitialize();
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
                                + " phone_number STRING"
                                + ("customers_no_pk".equals(tableName)
                                        ? ""
                                        : ", primary key (id) not enforced")
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + " %s"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        getTableNameRegex(captureCustomerTables),
                        scanStartupMode,
                        skipSnapshotBackfill,
                        getServerId(),
                        chunkColumnName == null
                                ? ""
                                : String.format(
                                        ", 'scan.incremental.snapshot.chunk.key-column' = '%s'",
                                        chunkColumnName));
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");

        // first step: check the snapshot data
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(tableResult, failoverType, failoverPhase, captureCustomerTables);
        }

        // second step: check the binlog data
        checkBinlogData(tableResult, failoverType, failoverPhase, captureCustomerTables);

        tableResult.getJobClient().get().cancel().get();
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
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
    }

    private void checkBinlogData(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        for (String tableId : captureCustomerTables) {
            makeFirstPartBinlogEvents(
                    getConnection(), customDatabase.getDatabaseName() + '.' + tableId);
        }

        // wait for the binlog reading
        Thread.sleep(2000L);

        if (failoverPhase == FailoverPhase.BINLOG) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartBinlogEvents(
                    getConnection(), customDatabase.getDatabaseName() + '.' + tableId);
        }

        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(firstPartBinlogEvents);
            expectedBinlogData.addAll(secondPartBinlogEvents);
        }

        assertEqualsInAnyOrder(expectedBinlogData, fetchRows(iterator, expectedBinlogData.size()));
        assertTrue(!hasNextData(iterator));
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("id", 0);
        map.put("name", 1);
        map.put("address", 2);
        map.put("phone_number", 3);
        return rows.stream()
                .map(
                        row ->
                                RowUtils.createRowWithNamedPositions(
                                                row.getRowKind(),
                                                new Object[] {
                                                    row.getLong(0),
                                                    row.getString(1),
                                                    row.getString(2),
                                                    row.getString(3)
                                                },
                                                map)
                                        .toString())
                .collect(Collectors.toList());
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

    private List<String> fetchRowData(
            Iterator<RowData> iter, int size, Function<RowData, String> stringifier) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return rows.stream().map(stringifier).collect(Collectors.toList());
    }

    private static List<String> fetchRowData(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return convertRowDataToRowString(rows);
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

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Make some changes on the specified customer table. Changelog in string could be accessed by
     * {@link #firstPartBinlogEvents}.
     */
    private void makeFirstPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for the first split
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
     * accessed by {@link #secondPartBinlogEvents}.
     */
    private void makeSecondPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
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

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    private String getTableId() {
        return customDatabase.getDatabaseName() + "." + tableName;
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

    /**
     * A {@link DebeziumDeserializationSchema} implementation which sleep given milliseconds after
     * deserialize per record, this class is designed for test.
     */
    static class SleepingRowDataDebeziumDeserializeSchema
            implements DebeziumDeserializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private final RowDataDebeziumDeserializeSchema deserializeSchema;
        private final long sleepMs;

        public SleepingRowDataDebeziumDeserializeSchema(
                RowDataDebeziumDeserializeSchema deserializeSchema, long sleepMs) {
            this.deserializeSchema = deserializeSchema;
            this.sleepMs = sleepMs;
        }

        @Override
        public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
            deserializeSchema.deserialize(record, out);
            Thread.sleep(sleepMs);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return deserializeSchema.getProducedType();
        }
    }
}
