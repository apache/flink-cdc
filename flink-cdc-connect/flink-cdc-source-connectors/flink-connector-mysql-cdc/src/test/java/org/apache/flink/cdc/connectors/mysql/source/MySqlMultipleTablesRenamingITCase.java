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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.utils.TestCaseUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTable;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTableSchemas;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;

import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Integration tests for handling schema changes regard to renaming multiple tables within a single
 * statement.
 */
class MySqlMultipleTablesRenamingITCase extends MySqlSourceTestBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlMultipleTablesRenamingITCase.class);

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
    private final TestTable customers =
            new TestTable(customDatabase, "customers", TestTableSchemas.CUSTOMERS);

    private MySqlConnection connection;

    @BeforeEach
    public void prepare() throws Exception {
        connection = getConnection();
        customDatabase.createAndInitialize();
        flushLogs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        customDatabase.dropDatabase();
        connection.close();
    }

    /**
     * Tests handling of renaming multiple tables within a single SQL statement in a Flink CDC job.
     *
     * <p>This integration test validates that schema changes involving multiple table renames, such
     * as {@code RENAME TABLE table1 TO table1_old, table2 TO table1}, are correctly processed
     * without data loss or inconsistency.
     *
     * <p>The test covers:
     *
     * <ul>
     *   <li>Initial validation of table contents before renaming.
     *   <li>Steps to rename tables, including schema changes like column drops.
     *   <li>Ensuring data integrity during savepoints and job restarts.
     *   <li>Validation of data consumption before and after savepoints to confirm state
     *       correctness.
     * </ul>
     *
     * <p>This ensures that the connector can accurately process and persist schema changes when
     * tables are swapped, addressing potential issues with table filtering or mismatched table IDs
     * during schema updates.
     */
    @Test
    void testRenameTablesWithinSingleStatement() throws Exception {
        // Build Flink job
        StreamExecutionEnvironment env = getExecutionEnvironment();
        MySqlSource<String> source = getSourceBuilder().build();
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "rename-tables-test");
        CollectResultIterator<String> iterator = addCollector(env, stream);

        // Copy transformations into another env
        StreamExecutionEnvironment restoredEnv = getExecutionEnvironment();
        duplicateTransformations(env, restoredEnv);

        // Execute job and validate results
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);

        {
            String[] expected =
                    new String[] {
                        "{\"id\":101,\"name\":\"user_1\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":102,\"name\":\"user_2\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":103,\"name\":\"user_3\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":109,\"name\":\"user_4\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":110,\"name\":\"user_5\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":111,\"name\":\"user_6\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":118,\"name\":\"user_7\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":121,\"name\":\"user_8\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":123,\"name\":\"user_9\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1009,\"name\":\"user_10\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1010,\"name\":\"user_11\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1011,\"name\":\"user_12\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1012,\"name\":\"user_13\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1013,\"name\":\"user_14\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1014,\"name\":\"user_15\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1015,\"name\":\"user_16\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1016,\"name\":\"user_17\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1017,\"name\":\"user_18\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1018,\"name\":\"user_19\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":1019,\"name\":\"user_20\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                        "{\"id\":2000,\"name\":\"user_21\",\"address\":\"Shanghai\",\"phone_number\":\"123567891234\"}",
                    };
            List<String> rows = fetchRow(iterator, 21);
            TestCaseUtils.repeatedCheck(
                    () -> Arrays.stream(expected).allMatch(rows.toString()::contains));
        }

        {
            LOG.info("Step 1: Create a copy of the target table");
            executeStatements(
                    String.format(
                            "CREATE TABLE %s_copy LIKE %s;",
                            customers.getTableId(), customers.getTableId()));

            LOG.info("Step 2: Alter the copied table to drop a column");
            executeStatements(
                    String.format(
                            "ALTER TABLE %s_copy DROP COLUMN phone_number;",
                            customers.getTableId()));

            LOG.info("Step 3: Swap the tables");
            executeStatements(
                    String.format(
                            "RENAME TABLE %s TO %s_old, %s_copy TO %s;",
                            customers.getTableId(),
                            customers.getTableId(),
                            customers.getTableId(),
                            customers.getTableId()));

            LOG.info("Step 4: Insert data into the altered table before the savepoint");
            executeStatements(
                    String.format(
                            "INSERT INTO %s VALUES (19213, 'Diana', 'Berlin');",
                            customers.getTableId()));

            List<String> rowsBeforeRestored = fetchRow(iterator, 1);
            TestCaseUtils.repeatedCheck(
                    () ->
                            rowsBeforeRestored
                                    .toString()
                                    .contains(
                                            "{\"id\":19213,\"name\":\"Diana\",\"address\":\"Berlin\"}"));
        }

        {
            LOG.info("Step 5: Take a savepoint");
            Path savepointDir = Files.createTempDirectory("rename-tables-test");
            String savepointPath =
                    jobClient
                            .stopWithSavepoint(
                                    false,
                                    savepointDir.toAbsolutePath().toString(),
                                    SavepointFormatType.DEFAULT)
                            .get();

            LOG.info("Step 6: Insert data into the altered table after the savepoint");
            executeStatements(
                    String.format(
                            "INSERT INTO %s VALUES (19214, 'Diana2', 'Berlin2');",
                            customers.getTableId()));

            LOG.info("Step 7: Restart the job from savepoint");
            setupSavepoint(restoredEnv, savepointPath);
            JobClient restoredJobClient = restoredEnv.executeAsync("rename-tables-test");
            iterator.setJobClient(restoredJobClient);
            List<String> rowsAfterRestored = fetchRow(iterator, 1);
            TestCaseUtils.repeatedCheck(
                    () ->
                            rowsAfterRestored
                                    .toString()
                                    .contains(
                                            "{\"id\":19214,\"name\":\"Diana2\",\"address\":\"Berlin2\"}"));
            restoredJobClient.cancel().get();
        }
    }

    private MySqlSourceBuilder<String> getSourceBuilder() {
        return MySqlSource.<String>builder()
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customDatabase.getUsername())
                .password(customDatabase.getPassword())
                .databaseList(customDatabase.getDatabaseName())
                .tableList(customers.getTableId())
                .serverId(getServerId())
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema());
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    private void executeStatements(String... statements) throws Exception {
        connection.execute(statements);
        connection.commit();
    }

    private void flushLogs() throws Exception {
        executeStatements("FLUSH LOGS;");
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

    private static List<String> fetchRow(Iterator<String> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            String row = iter.next();
            rows.add(row);
            size--;
        }
        return rows;
    }

    private void setupSavepoint(StreamExecutionEnvironment env, String savepointPath)
            throws Exception {
        // restore from savepoint
        // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> clazz =
                classLoader.loadClass(
                        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
        Field field = clazz.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = (Configuration) field.get(env);
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
    }

    private void duplicateTransformations(
            StreamExecutionEnvironment source, StreamExecutionEnvironment target) {
        source.getTransformations().forEach(target::addOperator);
    }

    private StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }
}
