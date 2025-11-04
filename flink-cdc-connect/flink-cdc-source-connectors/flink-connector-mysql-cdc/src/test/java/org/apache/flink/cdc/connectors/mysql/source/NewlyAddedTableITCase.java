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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests to cover various newly added tables during capture process. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class NewlyAddedTableITCase extends MySqlSourceTestBase {

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final ScheduledExecutorService mockBinlogExecutor = Executors.newScheduledThreadPool(1);

    @TempDir private Path tempFolder;

    @BeforeEach
    public void before() throws SQLException {
        TestValuesTableFactory.clearAllData();
        customDatabase.createAndInitialize();

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            // prepare initial data for given table
            String tableId = customDatabase.getDatabaseName() + ".produce_binlog_table";
            connection.execute(
                    format("CREATE TABLE %s ( id BIGINT PRIMARY KEY, cnt BIGINT);", tableId));
            connection.execute(
                    format("INSERT INTO  %s VALUES (0, 100), (1, 101), (2, 102);", tableId));
            connection.commit();

            // mock continuous binlog during the newly added table capturing process
            mockBinlogExecutor.schedule(
                    () -> {
                        try {
                            connection.execute(
                                    format("UPDATE  %s SET  cnt = cnt +1 WHERE id < 2;", tableId));
                            connection.commit();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    },
                    500,
                    TimeUnit.MICROSECONDS);
        }
    }

    @AfterEach
    public void after() {
        mockBinlogExecutor.shutdown();
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlogAndAutoCloseReader()
            throws Exception {
        Map<String, String> otherOptions = new HashMap<>();
        otherOptions.put("scan.incremental.close-idle-reader.enabled", "true");
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                otherOptions,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThrice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThriceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testJobManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveAndAddNewTable() throws Exception {
        // round 1 : table0 + table1 (customers_even_dist + customers)
        // round 2 : table0 + table2 (customers_even_dist + customers_1)
        String tableId0 = customDatabase.getDatabaseName() + ".customers_even_dist";
        String tableId1 = "customers";
        String tableId2 = "customers_\\d+";

        final String savepointDirectory = tempFolder.toString();

        String finishedSavePointPath = null;
        CollectResultIterator<RowData> iterator = null;
        for (int i = 0; i < 2; i++) {
            String changedTable = i == 0 ? tableId1 : "customers_1";
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, 4);

            RowDataDebeziumDeserializeSchema deserializer =
                    RowDataDebeziumDeserializeSchema.newBuilder()
                            .setMetadataConverters(
                                    new MetadataConverter[] {
                                        MySqlReadableMetadata.TABLE_NAME.getConverter()
                                    })
                            .setPhysicalRowType(
                                    (RowType)
                                            DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "id", DataTypes.BIGINT()),
                                                            DataTypes.FIELD(
                                                                    "name", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "address", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "phone_number",
                                                                    DataTypes.STRING()))
                                                    .getLogicalType())
                            .setResultTypeInfo(
                                    InternalTypeInfo.of(
                                            TypeConversions.fromDataToLogicalType(
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "id", DataTypes.BIGINT()),
                                                            DataTypes.FIELD(
                                                                    "name", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "address", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "phone_number",
                                                                    DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "_table_name",
                                                                    DataTypes.STRING()
                                                                            .notNull())))))
                            .build();

            // Build source
            MySqlSource<RowData> mySqlSource =
                    MySqlSource.<RowData>builder()
                            .hostname(MYSQL_CONTAINER.getHost())
                            .port(MYSQL_CONTAINER.getDatabasePort())
                            .databaseList(customDatabase.getDatabaseName())
                            .serverTimeZone("UTC")
                            .tableList(
                                    tableId0,
                                    customDatabase.getDatabaseName()
                                            + "."
                                            + (i == 0 ? tableId1 : tableId2))
                            .username(customDatabase.getUsername())
                            .password(customDatabase.getPassword())
                            .serverId("5401-5404")
                            .deserializer(deserializer)
                            .scanNewlyAddedTableEnabled(true)
                            .build();

            // Build and execute the job
            DataStreamSource<RowData> source =
                    env.fromSource(
                            mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source" + i);
            if (iterator == null) {
                iterator = addCollectSink(source);
            } else {
                addCollectSink(source);
            }
            JobClient jobClient = env.executeAsync("Collect " + i);
            iterator.setJobClient(jobClient);

            List<String> expectedCustomersEvenDistResult =
                    Arrays.asList(
                            "+I[103, user_3, Shanghai, 123567891234, customers_even_dist]",
                            "+I[104, user_4, Shanghai, 123567891234, customers_even_dist]",
                            "+I[101, user_1, Shanghai, 123567891234, customers_even_dist]",
                            "+I[102, user_2, Shanghai, 123567891234, customers_even_dist]",
                            "+I[107, user_7, Shanghai, 123567891234, customers_even_dist]",
                            "+I[108, user_8, Shanghai, 123567891234, customers_even_dist]",
                            "+I[105, user_5, Shanghai, 123567891234, customers_even_dist]",
                            "+I[106, user_6, Shanghai, 123567891234, customers_even_dist]",
                            "+I[109, user_9, Shanghai, 123567891234, customers_even_dist]",
                            "+I[110, user_10, Shanghai, 123567891234, customers_even_dist]");
            List<String> expectedCustomersResult =
                    Arrays.asList(
                            format("+I[1011, user_12, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1012, user_13, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1009, user_10, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1010, user_11, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1015, user_16, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1016, user_17, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1013, user_14, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[118, user_7, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1014, user_15, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[111, user_6, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[2000, user_21, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[109, user_4, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[110, user_5, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[103, user_3, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[101, user_1, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[102, user_2, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[123, user_9, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1019, user_20, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[121, user_8, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1017, user_18, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[1018, user_19, Shanghai, 123567891234, %s]", changedTable));
            List<String> expectedBinlogResult =
                    Arrays.asList(
                            format("-U[103, user_3, Shanghai, 123567891234, %s]", changedTable),
                            format("+U[103, user_3, Update1, 123567891234, %s]", changedTable),
                            format("-D[102, user_2, Shanghai, 123567891234, %s]", changedTable),
                            format("+I[102, user_2, Insert1, 123567891234, %s]", changedTable),
                            format("-U[103, user_3, Update1, 123567891234, %s]", changedTable),
                            format("+U[103, user_3, Update2, 123567891234, %s]", changedTable));

            List<String> expectedSnapshotResult =
                    i == 0
                            ? Stream.concat(
                                            expectedCustomersEvenDistResult.stream(),
                                            expectedCustomersResult.stream())
                                    .collect(Collectors.toList())
                            : expectedCustomersResult;
            List<String> rows = fetchRowData(iterator, expectedSnapshotResult.size());
            assertEqualsInAnyOrder(expectedSnapshotResult, rows);

            // make binlog events
            try (MySqlConnection connection = getConnection()) {
                connection.setAutoCommit(false);
                String tableId = customDatabase.getDatabaseName() + "." + changedTable;
                connection.execute(
                        "UPDATE " + tableId + " SET address = 'Update1' where id = 103",
                        "DELETE FROM " + tableId + " where id = 102",
                        "INSERT INTO "
                                + tableId
                                + " VALUES(102, 'user_2','Insert1','123567891234')",
                        "UPDATE " + tableId + " SET address = 'Update2' where id = 103");
                connection.commit();
            }
            rows = fetchRowData(iterator, expectedBinlogResult.size());
            assertEqualsInAnyOrder(expectedBinlogResult, rows);

            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }
    }

    @Test
    void testNewlyAddedEmptyTableAndInsertAfterJobStart() throws Exception {
        testNewlyAddedTableOneByOneWithCreateBeforeStart(
                1, new HashMap<>(), "address_hangzhou", "address_beijing");
    }

    /** Add a collect sink in the job. */
    protected CollectResultIterator<RowData> addCollectSink(DataStream<RowData> stream) {
        TypeSerializer<RowData> serializer =
                stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<RowData> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<RowData> operator =
                (CollectSinkOperator<RowData>) factory.getOperator();
        CollectStreamSink<RowData> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        CollectResultIterator<RowData> iterator =
                new CollectResultIterator(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        stream.getExecutionEnvironment().getCheckpointConfig(),
                        10000L);
        return iterator;
    }

    private List<String> fetchRowData(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return convertRowDataToRowString(rows);
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("id", 0);
        map.put("name", 1);
        map.put("address", 2);
        map.put("phone_number", 3);
        map.put("_table_name", 4);
        return rows.stream()
                .map(
                        row ->
                                RowUtils.createRowWithNamedPositions(
                                                row.getRowKind(),
                                                new Object[] {
                                                    row.getLong(0),
                                                    row.getString(1),
                                                    row.getString(2),
                                                    row.getString(3),
                                                    row.getString(4)
                                                },
                                                map)
                                        .toString())
                .collect(Collectors.toList());
    }

    private void testRemoveTablesOneByOne(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String... captureAddressTables)
            throws Exception {

        // step 1: create mysql tables with all tables included
        initialAddressTables(getConnection(), captureAddressTables);

        final String savepointDirectory = tempFolder.toString();

        // get all expected data
        List<String> fetchedDataList = new ArrayList<>();
        for (String table : captureAddressTables) {
            String cityName = table.split("_")[1];
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    table, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    table, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    table, cityName, cityName)));
        }

        String finishedSavePointPath = null;
        // step 2: execute insert and trigger savepoint with all tables added
        {
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureAddressTables);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // trigger failover after some snapshot data read finished
            if (failoverPhase == FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // sleep 1s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
            // Otherwise, the restart job won't read newly added tables, and this test will be
            // stuck.
            Thread.sleep(1000L);
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }

        // test removing table one by one, note that there should be at least one table remaining
        for (int round = 0; round < captureAddressTables.length - 1; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(round + 1, captureAddressTables.length)
                            .toArray(new String[0]);

            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 3: make binlog data for all tables
            List<String> expectedBinlogDataThisRound = new ArrayList<>();

            for (int i = 0, captureAddressTablesLength = captureAddressTables.length;
                    i < captureAddressTablesLength;
                    i++) {
                String tableName = captureAddressTables[i];
                makeBinlogForAddressTable(getConnection(), tableName, round);
                if (i <= round) {
                    continue;
                }
                String cityName = tableName.split("_")[1];
                expectedBinlogDataThisRound.addAll(
                        Arrays.asList(
                                format(
                                        "+U[%s, 416874195632735147, CHINA_%s, %s, %s West Town address 1]",
                                        tableName, round, cityName, cityName),
                                format(
                                        "+I[%s, %d, China, %s, %s West Town address 4]",
                                        tableName,
                                        417022095255614380L + round,
                                        cityName,
                                        cityName)));
            }

            if (failoverPhase == FailoverPhase.BINLOG
                    && TestValuesTableFactory.getRawResultsAsStrings("sink").size()
                            > fetchedDataList.size()) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }

            fetchedDataList.addAll(expectedBinlogDataThisRound);
            // step 4: assert fetched binlog data in this round
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 5: trigger savepoint
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressTables)
            throws Exception {
        testNewlyAddedTableOneByOne(
                parallelism,
                new HashMap<>(),
                failoverType,
                failoverPhase,
                makeBinlogBeforeCapture,
                captureAddressTables);
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            Map<String, String> sourceOptions,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressTables)
            throws Exception {

        // step 1: create mysql tables with initial data
        initialAddressTables(getConnection(), captureAddressTables);

        final String savepointDirectory = tempFolder.toString();

        // test newly added table one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            if (makeBinlogBeforeCapture) {
                makeBinlogBeforeCaptureForAddressTable(getConnection(), newlyAddedTable);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedTable.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedTable, cityName, cityName));
            if (makeBinlogBeforeCapture) {
                expectedSnapshotDataThisRound =
                        Arrays.asList(
                                format(
                                        "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614381, China, %s, %s West Town address 5]",
                                        newlyAddedTable, cityName, cityName));
            }

            // trigger failover after some snapshot data read finished
            if (failoverPhase == FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // Wait 1s until snapshot phase finished, make sure the binlog data is not lost.
            Thread.sleep(1000L);

            // step 3: make some binlog data for this round
            makeFirstPartBinlogForAddressTable(getConnection(), newlyAddedTable);
            if (failoverPhase == FailoverPhase.BINLOG) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartBinlogForAddressTable(getConnection(), newlyAddedTable);

            // step 4: assert fetched binlog data in this round

            // retract the old data with id 416874195632735147
            fetchedDataList =
                    fetchedDataList.stream()
                            .filter(
                                    r ->
                                            !r.contains(
                                                    format(
                                                            "%s, 416874195632735147",
                                                            newlyAddedTable)))
                            .collect(Collectors.toList());
            List<String> expectedBinlogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));

            // step 5: assert fetched binlog data in this round
            fetchedDataList.addAll(expectedBinlogUpsertDataThisRound);

            waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000);
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressTables.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private String getCreateTableStatement(
            Map<String, String> otherOptions, String... captureTableNames) {
        return format(
                "CREATE TABLE address ("
                        + " table_name STRING METADATA VIRTUAL,"
                        + " id BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (city, id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '2',"
                        + " 'chunk-meta.group.size' = '2',"
                        + " 'server-time-zone' = 'UTC',"
                        + " 'server-id' = '%s',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + " %s"
                        + ")",
                MYSQL_CONTAINER.getHost(),
                MYSQL_CONTAINER.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                getTableNameRegex(captureTableNames),
                getServerId(),
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

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) throws Exception {
        Configuration configuration = new Configuration();
        if (finishedSavePointPath != null) {
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
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

    private void initialAddressTables(JdbcConnection connection, String[] addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = customDatabase.getDatabaseName() + "." + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE "
                                + tableId
                                + "("
                                + "  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                                + "  country VARCHAR(255) NOT NULL,"
                                + "  city VARCHAR(255) NOT NULL,"
                                + "  detail_address VARCHAR(1024)"
                                + ");");
                connection.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                        + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                        + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                                tableId, cityName, cityName, cityName, cityName, cityName,
                                cityName));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeFirstPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the first split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'CHINA' where id = 416874195632735147",
                            tableId));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the second split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614380, 'China','%s','%s West Town address 4')",
                            tableId, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeBinlogBeforeCaptureForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog before the capture of the table
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614381, 'China','%s','%s West Town address 5')",
                            tableId, cityName, cityName));
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

    private void makeBinlogForAddressTable(JdbcConnection connection, String tableName, int round)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the first split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'CHINA_%s' where id = 416874195632735147",
                            tableId, round));
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(%d, 'China','%s','%s West Town address 4')",
                            tableId, 417022095255614380L + round, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private void testNewlyAddedTableOneByOneWithCreateBeforeStart(
            int parallelism, Map<String, String> sourceOptions, String... captureAddressTables)
            throws Exception {
        final String savepointDirectory = tempFolder.toString();
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            boolean insertData = round == 0;
            initialAddressTables(getConnection(), captureAddressTables, round, insertData);
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            env.setRestartStrategy(noRestart());
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();
            Thread.sleep(3_000);
            String tableName = captureAddressTables[round];
            if (!insertData) {
                insertData(
                        getConnection(),
                        customDatabase.getDatabaseName() + "." + tableName,
                        tableName.split("_")[1]);
            }
            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedTable.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedTable, cityName, cityName));
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // step 3: make some binlog data for this round
            makeFirstPartBinlogForAddressTable(getConnection(), newlyAddedTable);
            makeSecondPartBinlogForAddressTable(getConnection(), newlyAddedTable);
            // step 4: assert fetched binlog data in this round
            // retract the old data with id 416874195632735147
            fetchedDataList =
                    fetchedDataList.stream()
                            .filter(
                                    r ->
                                            !r.contains(
                                                    format(
                                                            "%s, 416874195632735147",
                                                            newlyAddedTable)))
                            .collect(Collectors.toList());
            List<String> expectedBinlogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));
            // step 5: assert fetched binlog data in this round
            fetchedDataList.addAll(expectedBinlogUpsertDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000);
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // step 6: trigger savepoint
            if (round != captureAddressTables.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private void initialAddressTables(
            JdbcConnection connection, String[] addressTables, int round, boolean insertData)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            String tableName = addressTables[round];
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    "CREATE TABLE "
                            + tableId
                            + "("
                            + "  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                            + "  country VARCHAR(255) NOT NULL,"
                            + "  city VARCHAR(255) NOT NULL,"
                            + "  detail_address VARCHAR(1024)"
                            + ");");
            if (insertData) {
                insertData(connection, tableId, cityName);
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void insertData(JdbcConnection connection, String tableId, String cityName)
            throws SQLException {
        try {
            connection.execute(
                    format(
                            "INSERT INTO  %s "
                                    + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                    + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                    + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                            tableId, cityName, cityName, cityName, cityName, cityName, cityName));
        } finally {
            connection.close();
        }
    }
}
