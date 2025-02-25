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
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.TestCaseUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.cdc.common.utils.TestCaseUtils.DEFAULT_INTERVAL;
import static org.apache.flink.cdc.common.utils.TestCaseUtils.DEFAULT_TIMEOUT;

/**
 * IT case for Evolving MySQL schema with gh-ost/pt-osc utility. See <a
 * href="https://github.com/github/gh-ost">github/gh-ost</a>/<a
 * href="https://docs.percona.com/percona-toolkit/pt-online-schema-change.html">doc/pt-osc</a> for
 * more details.
 */
class MySqlOnLineSchemaMigrationSourceITCase extends MySqlSourceTestBase {
    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private static final String PERCONA_TOOLKIT = "perconalab/percona-toolkit:3.5.7";

    protected static final GenericContainer<?> PERCONA_TOOLKIT_CONTAINER =
            createPerconaToolkitContainer();

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "customer", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private static final String GH_OST_DOWNLOAD_LINK =
            DockerClientFactory.instance().client().versionCmd().exec().getArch().equals("amd64")
                    ? "https://github.com/github/gh-ost/releases/download/v1.1.6/gh-ost-binary-linux-amd64-20231207144046.tar.gz"
                    : "https://github.com/github/gh-ost/releases/download/v1.1.6/gh-ost-binary-linux-arm64-20231207144046.tar.gz";

    @BeforeAll
    static void beforeClass() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        Startables.deepStart(Stream.of(PERCONA_TOOLKIT_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void afterClass() {
        LOG.info("Stopping containers...");
        MYSQL8_CONTAINER.stop();
        PERCONA_TOOLKIT_CONTAINER.close();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        customerDatabase.createAndInitialize();
        System.setOut(new PrintStream(outCaptor));
    }

    @AfterEach
    void after() {
        customerDatabase.dropDatabase();
        System.setOut(sysOut);
    }

    private static void installGhOstCli(Container<?> container) {
        try {
            execInContainer(
                    container,
                    "download gh-ost tarball",
                    "curl",
                    "-L",
                    "-o",
                    "/tmp/gh-ost.tar.gz",
                    GH_OST_DOWNLOAD_LINK);
            execInContainer(
                    container, "unzip binary", "tar", "-xzvf", "/tmp/gh-ost.tar.gz", "-C", "/bin");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericContainer<?> createPerconaToolkitContainer() {
        GenericContainer<?> perconaToolkit =
                new GenericContainer<>(PERCONA_TOOLKIT)
                        // keep container alive
                        .withCommand("tail", "-f", "/dev/null")
                        .withNetwork(NETWORK)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        return perconaToolkit;
    }

    private final PrintStream sysOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @Test
    void testGhOstSchemaMigrationFromScratch() throws Exception {
        LOG.info("Step 1: Install gh-ost command line utility");
        installGhOstCli(MYSQL8_CONTAINER);

        LOG.info("Step 2: Start pipeline job");
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .databaseList(customerDatabase.getDatabaseName())
                        .tableList(customerDatabase.getDatabaseName() + ".customers")
                        .username(customerDatabase.getUsername())
                        .password(customerDatabase.getPassword())
                        .serverId(getServerId())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        JobClient resultClient = env.executeAsync();

        TestCaseUtils.repeatedCheck(
                () -> resultClient.getJobStatus().get().equals(RUNNING),
                DEFAULT_TIMEOUT,
                DEFAULT_INTERVAL,
                Arrays.asList(InterruptedException.class, NoSuchElementException.class));

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
            TestCaseUtils.repeatedCheck(
                    () -> Arrays.stream(expected).allMatch(outCaptor.toString()::contains));
        }

        // Wait for a little while until we're in Binlog streaming mode.
        Thread.sleep(5_000);

        {
            LOG.info("Step 3: Evolve schema with gh-ost - ADD COLUMN");
            execInContainer(
                    MYSQL8_CONTAINER,
                    "evolve schema",
                    "gh-ost",
                    "--user=" + TEST_USER,
                    "--password=" + TEST_PASSWORD,
                    "--database=" + customerDatabase.getDatabaseName(),
                    "--table=customers",
                    "--alter=add column ext int first",
                    "--allow-on-master", // because we don't have a replica
                    "--initially-drop-old-table", // drop previously generated temporary tables
                    "--execute");

            try (Connection connection = customerDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // The new column `ext` has been inserted now
                statement.execute(
                        String.format(
                                "INSERT INTO `%s`.`customers` VALUES (17, 10000, 'Alice', 'Beijing', '123567891234');",
                                customerDatabase.getDatabaseName()));
            }

            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"ext\":17,\"id\":10000,\"name\":\"Alice\",\"address\":\"Beijing\",\"phone_number\":\"123567891234\"}"));
        }

        {
            LOG.info("Step 4: Evolve schema with gh-ost - MODIFY COLUMN");
            execInContainer(
                    MYSQL8_CONTAINER,
                    "evolve schema",
                    "gh-ost",
                    "--user=" + TEST_USER,
                    "--password=" + TEST_PASSWORD,
                    "--database=" + customerDatabase.getDatabaseName(),
                    "--table=customers",
                    "--alter=modify column ext double",
                    "--allow-on-master",
                    "--initially-drop-old-table",
                    "--execute");

            try (Connection connection = customerDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        String.format(
                                "INSERT INTO `%s`.`customers` VALUES (2.718281828, 10001, 'Bob', 'Chongqing', '123567891234');",
                                customerDatabase.getDatabaseName()));
            }

            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"ext\":2.718281828,\"id\":10001,\"name\":\"Bob\",\"address\":\"Chongqing\",\"phone_number\":\"123567891234\"}"));
        }

        {
            LOG.info("Step 5: Evolve schema with gh-ost - DROP COLUMN");
            execInContainer(
                    MYSQL8_CONTAINER,
                    "evolve schema",
                    "gh-ost",
                    "--user=" + TEST_USER,
                    "--password=" + TEST_PASSWORD,
                    "--database=" + customerDatabase.getDatabaseName(),
                    "--table=customers",
                    "--alter=drop column ext",
                    "--allow-on-master",
                    "--initially-drop-old-table",
                    "--execute");

            try (Connection connection = customerDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        String.format(
                                "INSERT INTO `%s`.`customers` VALUES (10002, 'Cicada', 'Urumqi', '123567891234');",
                                customerDatabase.getDatabaseName()));
            }

            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"id\":10002,\"name\":\"Cicada\",\"address\":\"Urumqi\",\"phone_number\":\"123567891234\"}"));
        }
        resultClient.cancel();
    }

    @Test
    void testPtOscSchemaMigrationFromScratch() throws Exception {
        LOG.info("Step 1: Start pipeline job");
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .databaseList(customerDatabase.getDatabaseName())
                        .tableList(customerDatabase.getDatabaseName() + ".customers")
                        .username(customerDatabase.getUsername())
                        .password(customerDatabase.getPassword())
                        .serverId(getServerId())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        JobClient resultClient = env.executeAsync();

        TestCaseUtils.repeatedCheck(
                () -> resultClient.getJobStatus().get().equals(RUNNING),
                DEFAULT_TIMEOUT,
                DEFAULT_INTERVAL,
                Arrays.asList(InterruptedException.class, NoSuchElementException.class));

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
            TestCaseUtils.repeatedCheck(
                    () -> Arrays.stream(expected).allMatch(outCaptor.toString()::contains));
        }

        // Wait for a little while until we're in Binlog streaming mode.
        Thread.sleep(5000L);

        LOG.info("Step 2: Evolve schema with pt-osc - ADD COLUMN");
        execInContainer(
                PERCONA_TOOLKIT_CONTAINER,
                "evolve schema",
                "pt-online-schema-change",
                "--user=" + TEST_USER,
                "--host=" + INTER_CONTAINER_MYSQL_ALIAS,
                "--password=" + TEST_PASSWORD,
                "P=3306,t=customers,D=" + customerDatabase.getDatabaseName(),
                "--alter",
                "add column ext int FIRST",
                "--charset=utf8",
                "--recursion-method=NONE", // Do not look for slave nodes
                "--print",
                "--execute");

        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            // The new column `ext` has been inserted now
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` VALUES (17, 10000, 'Alice', 'Beijing', '123567891234');",
                            customerDatabase.getDatabaseName()));

            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"ext\":17,\"id\":10000,\"name\":\"Alice\",\"address\":\"Beijing\",\"phone_number\":\"123567891234\"}"));
        }

        LOG.info("Step 3: Evolve schema with pt-osc - MODIFY COLUMN");
        execInContainer(
                PERCONA_TOOLKIT_CONTAINER,
                "evolve schema",
                "pt-online-schema-change",
                "--user=" + TEST_USER,
                "--host=" + INTER_CONTAINER_MYSQL_ALIAS,
                "--password=" + TEST_PASSWORD,
                "P=3306,t=customers,D=" + customerDatabase.getDatabaseName(),
                "--alter",
                "modify column ext double",
                "--charset=utf8",
                "--recursion-method=NONE",
                "--print",
                "--execute");

        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` VALUES (2.718281828, 10001, 'Bob', 'Chongqing', '123567891234');",
                            customerDatabase.getDatabaseName()));
            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"ext\":2.718281828,\"id\":10001,\"name\":\"Bob\",\"address\":\"Chongqing\",\"phone_number\":\"123567891234\"}"));
        }

        LOG.info("Step 4: Evolve schema with pt-osc - DROP COLUMN");
        execInContainer(
                PERCONA_TOOLKIT_CONTAINER,
                "evolve schema",
                "pt-online-schema-change",
                "--user=" + TEST_USER,
                "--host=" + INTER_CONTAINER_MYSQL_ALIAS,
                "--password=" + TEST_PASSWORD,
                "P=3306,t=customers,D=" + customerDatabase.getDatabaseName(),
                "--alter",
                "drop column ext",
                "--charset=utf8",
                "--recursion-method=NONE",
                "--print",
                "--execute");

        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` VALUES (10002, 'Cicada', 'Urumqi', '123567891234');",
                            customerDatabase.getDatabaseName()));
            TestCaseUtils.repeatedCheck(
                    () ->
                            outCaptor
                                    .toString()
                                    .contains(
                                            "{\"id\":10002,\"name\":\"Cicada\",\"address\":\"Urumqi\",\"phone_number\":\"123567891234\"}"));
        }
    }

    private static void execInContainer(Container<?> container, String prompt, String... commands)
            throws IOException, InterruptedException {
        {
            LOG.info(
                    "Starting to {} with the following command: `{}`",
                    prompt,
                    String.join(" ", commands));
            Container.ExecResult execResult = container.execInContainer(commands);
            if (execResult.getExitCode() == 0) {
                LOG.info("Successfully {}. Stdout: {}", prompt, execResult.getStdout());
            } else {
                LOG.error(
                        "Failed to {}. Exit code: {}, Stdout: {}, Stderr: {}",
                        prompt,
                        execResult.getExitCode(),
                        execResult.getStdout(),
                        execResult.getStderr());
                throw new IOException("Failed to execute commands: " + String.join(" ", commands));
            }
        }
    }

    private List<Event> getSnapshotExpected(TableId tableId, Schema schema) {
        return Stream.of(
                        generate(schema, 101, "user_1", "Shanghai", "123567891234"),
                        generate(schema, 102, "user_2", "Shanghai", "123567891234"),
                        generate(schema, 103, "user_3", "Shanghai", "123567891234"),
                        generate(schema, 109, "user_4", "Shanghai", "123567891234"),
                        generate(schema, 110, "user_5", "Shanghai", "123567891234"),
                        generate(schema, 111, "user_6", "Shanghai", "123567891234"),
                        generate(schema, 118, "user_7", "Shanghai", "123567891234"),
                        generate(schema, 121, "user_8", "Shanghai", "123567891234"),
                        generate(schema, 123, "user_9", "Shanghai", "123567891234"),
                        generate(schema, 1009, "user_10", "Shanghai", "123567891234"),
                        generate(schema, 1010, "user_11", "Shanghai", "123567891234"),
                        generate(schema, 1011, "user_12", "Shanghai", "123567891234"),
                        generate(schema, 1012, "user_13", "Shanghai", "123567891234"),
                        generate(schema, 1013, "user_14", "Shanghai", "123567891234"),
                        generate(schema, 1014, "user_15", "Shanghai", "123567891234"),
                        generate(schema, 1015, "user_16", "Shanghai", "123567891234"),
                        generate(schema, 1016, "user_17", "Shanghai", "123567891234"),
                        generate(schema, 1017, "user_18", "Shanghai", "123567891234"),
                        generate(schema, 1018, "user_19", "Shanghai", "123567891234"),
                        generate(schema, 1019, "user_20", "Shanghai", "123567891234"),
                        generate(schema, 2000, "user_21", "Shanghai", "123567891234"))
                .map(record -> DataChangeEvent.insertEvent(tableId, record))
                .collect(Collectors.toList());
    }

    private BinaryRecordData generate(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }
}
