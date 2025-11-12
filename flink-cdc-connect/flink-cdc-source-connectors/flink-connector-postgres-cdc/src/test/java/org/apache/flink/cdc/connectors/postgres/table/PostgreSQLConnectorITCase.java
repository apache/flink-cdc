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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.utils.StaticExternalResourceProxy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for PostgreSQL Table source. */
class PostgreSQLConnectorITCase extends PostgresTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    /** Use postgis plugin to test the GIS type. */
    protected static final DockerImageName POSTGIS_IMAGE =
            DockerImageName.parse("postgis/postgis:14-3.5").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGIS_CONTAINER =
            new PostgreSQLContainer<>(POSTGIS_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    @RegisterExtension
    public static StaticExternalResourceProxy<LegacyRowResource> usesLegacyRows =
            new StaticExternalResourceProxy<>(LegacyRowResource.INSTANCE);

    @BeforeAll
    static void startContainers() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER, POSTGIS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGIS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    void setup(boolean parallelismSnapshot) {
        TestValuesTableFactory.clearAllData();
        env.setRestartStrategy(RestartStrategies.noRestart());
        if (parallelismSnapshot) {
            env.setParallelism(4);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConsumingAllEvents(boolean parallelismSnapshot)
            throws SQLException, ExecutionException, InterruptedException {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");

        waitForSnapshotStarted("sink");

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | id | name | description | weight |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | 101 | scooter | Small 2-wheel scooter | 3.14 |
         * | 102 | car battery | 12V car battery | 8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from
         * #40 to #3 | 0.8 |
         * | 104 | hammer | 12oz carpenter's hammer | 0.75 |
         * | 105 | hammer | 14oz carpenter's hammer | 0.875 |
         * | 106 | hammer | 18oz carpenter hammer | 1 |
         * | 107 | rocks | box of assorted rocks | 5.1 |
         * | 108 | jacket | water resistent black wind breaker | 0.1 |
         * | 109 | spare tire | 24 inch spare tire | 22.2 |
         * | 110 | jacket | new water resistent white wind breaker | 0.5 |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * </pre>
         */

        String[] expected =
                new String[] {
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConsumingAllEventsForPartitionedTable(boolean parallelismSnapshot)
            throws SQLException, ExecutionException, InterruptedException {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory_partitioned");
        String publicationName = "dbz_publication_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products "
                                    + " WITH (publish_via_partition_root=true)",
                            publicationName));
            statement.execute(
                    String.format(
                            "select pg_create_logical_replication_slot('%s','pgoutput');",
                            slotName));
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " country STRING"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'scan.include-partitioned-tables.enabled' = 'true',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'debezium.publication.name'  = '%s',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory_partitioned",
                        "products",
                        parallelismSnapshot,
                        publicationName,
                        slotName);
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " country STRING,"
                        + " PRIMARY KEY (id, country) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT id, name, description, weight, country FROM debezium_source");

        waitForSnapshotStarted("sink");

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES (default,'jacket','water resistent white wind breaker',0.2, 'us');"); // 110
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, 'uk');");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_china PARTITION OF inventory_partitioned.products FOR VALUES IN ('china');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES (default,'bike','Big 2-wheel bycicle ',6.18, 'china');");
        }

        waitForSinkSize("sink", 11);

        // consume both snapshot and wal events
        String[] expected =
                new String[] {
                    "101,scooter,Small 2-wheel scooter,3.140,us",
                    "102,car battery,12V car battery,8.100,us",
                    "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800,us",
                    "104,hammer,12oz carpenter's hammer,0.750,us",
                    "105,hammer,14oz carpenter's hammer,0.875,us",
                    "106,hammer,16oz carpenter's hammer,1.000,uk",
                    "107,rocks,box of assorted rocks,5.300,uk",
                    "108,jacket,water resistent black wind breaker,0.100,uk",
                    "109,spare tire,24 inch spare tire,22.200,uk",
                    "110,jacket,water resistent white wind breaker,0.200,us",
                    "111,scooter,Big 2-wheel scooter ,5.180,uk",
                    "112,bike,Big 2-wheel bycicle ,6.180,china"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true})
    void testStartupFromLatestOffset(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        // wait for the source startup, we don't have a better way to wait it, use sleep
        // for now
        Thread.sleep(10000L);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"110,jacket,new water resistent white wind breaker,0.500"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testStartupFromCommittedOffset() throws Exception {
        setup(true);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'first','first description',0.1);");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'second','second description',0.2);");
        }

        // newly create slot's confirmed lsn is latest. We will test whether committed mode starts
        // from here.
        String slotName = getSlotName();
        String publicName = "dbz_publication_" + new Random().nextInt(1000);
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            // For pgoutput specifically, the publication must be created before the slot.
            // postgres community is still working on it:
            // https://www.postgresql.org/message-id/CALDaNm0-n8FGAorM%2BbTxkzn%2BAOUyx5%3DL_XmnvOP6T24%2B-NcBKg%40mail.gmail.com
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory.products", publicName));
            statement.execute(
                    String.format(
                            "select pg_create_logical_replication_slot('%s','pgoutput');",
                            slotName));
        }

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'thirth','thirth description',0.1);");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'forth','forth description',0.2);");
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'decoding.plugin.name' = 'pgoutput',"
                                + " 'slot.name' = '%s',"
                                + " 'debezium.publication.name'  = '%s',"
                                + " 'scan.lsn-commit.checkpoints-num-delay' = '0',"
                                + " 'scan.startup.mode' = 'committed-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        slotName,
                        publicName);
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        waitForSinkSize("sink", 2);

        String[] expected =
                new String[] {
                    "112,thirth,thirth description,0.100", "113,forth,forth description,0.200"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionForReplicaIdentity(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "replica_identity");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");
        waitForSnapshotStarted("sink");

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        Assertions.assertThatThrownBy(result::await)
                .hasStackTraceContaining(
                        "The \"before\" field of UPDATE/DELETE message is null, "
                                + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllTypes(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types ("
                                + "    id INTEGER NOT NULL,"
                                + "    bytea_c BYTES,"
                                + "    small_c SMALLINT,"
                                + "    int_c INTEGER,"
                                + "    big_c BIGINT,"
                                + "    real_c FLOAT,"
                                + "    double_precision DOUBLE,"
                                + "    numeric_c DECIMAL(10, 5),"
                                + "    decimal_c DECIMAL(10, 1),"
                                + "    boolean_c BOOLEAN,"
                                + "    text_c STRING,"
                                + "    char_c STRING,"
                                + "    character_c STRING,"
                                + "    character_varying_c STRING,"
                                + "    timestamp3_c TIMESTAMP(3),"
                                + "    timestamp6_c TIMESTAMP(6),"
                                + "    date_c DATE,"
                                + "    time_c TIME(0),"
                                + "    default_numeric_c DECIMAL,"
                                + "    geography_c STRING,"
                                + "    geometry_c STRING"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGIS_CONTAINER.getHost(),
                        POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGIS_CONTAINER.getUsername(),
                        POSTGIS_CONTAINER.getPassword(),
                        POSTGIS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "full_types",
                        parallelismSnapshot,
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    id INTEGER NOT NULL,"
                        + "    bytea_c BYTES,"
                        + "    small_c SMALLINT,"
                        + "    int_c INTEGER,"
                        + "    big_c BIGINT,"
                        + "    real_c FLOAT,"
                        + "    double_precision DOUBLE,"
                        + "    numeric_c DECIMAL(10, 5),"
                        + "    decimal_c DECIMAL(10, 1),"
                        + "    boolean_c BOOLEAN,"
                        + "    text_c STRING,"
                        + "    char_c STRING,"
                        + "    character_c STRING,"
                        + "    character_varying_c STRING,"
                        + "    timestamp3_c TIMESTAMP(3),"
                        + "    timestamp6_c TIMESTAMP(6),"
                        + "    date_c DATE,"
                        + "    time_c TIME(0),"
                        + "    default_numeric_c DECIMAL,"
                        + "    geography_c STRING,"
                        + "    geometry_c STRING,"
                        + "    PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        waitForSinkSize("sink", 1);
        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGIS_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE inventory.full_types SET small_c=0 WHERE id=1;");
        }

        waitForSinkSize("sink", 3);

        List<String> expected =
                Arrays.asList(
                        "+I(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "-D(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "+I(1,[50],0,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).isEqualTo(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadataColumns(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source  ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " schema_name STRING METADATA VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " row_kind STRING METADATA FROM 'row_kind' VIRTUAL,"
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " row_kind STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // sync submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        waitForSnapshotStarted("sink");
        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        // waiting for change events finished.
        waitForSinkSize("sink", 16);
        String databaseName = POSTGRES_CONTAINER.getDatabaseName();

        List<String> expected =
                Arrays.asList(
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,101,scooter,Small 2-wheel scooter,3.140)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,102,car battery,12V car battery,8.100)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,104,hammer,12oz carpenter's hammer,0.750)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,105,hammer,14oz carpenter's hammer,0.875)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,106,hammer,16oz carpenter's hammer,1.000)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,107,rocks,box of assorted rocks,5.300)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,108,jacket,water resistent black wind breaker,0.100)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,109,spare tire,24 inch spare tire,22.200)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,110,jacket,water resistent white wind breaker,0.200)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,111,scooter,Big 2-wheel scooter ,5.180)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,106,hammer,18oz carpenter hammer,1.000)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,107,rocks,box of assorted rocks,5.100)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,110,jacket,new water resistent white wind breaker,0.500)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,111,scooter,Big 2-wheel scooter ,5.170)",
                        "-D("
                                + databaseName
                                + ",inventory,products,-D,111,scooter,Big 2-wheel scooter ,5.170)");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUpsertMode(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "replica_identity");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'changelog-mode' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        getSlotName(),
                        parallelismSnapshot,
                        "upsert");
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");
        waitForSnapshotStarted("sink");

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | id | name | description | weight |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | 101 | scooter | Small 2-wheel scooter | 3.14 |
         * | 102 | car battery | 12V car battery | 8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from
         * #40 to #3 | 0.8 |
         * | 104 | hammer | 12oz carpenter's hammer | 0.75 |
         * | 105 | hammer | 14oz carpenter's hammer | 0.875 |
         * | 106 | hammer | 18oz carpenter hammer | 1 |
         * | 107 | rocks | box of assorted rocks | 5.1 |
         * | 108 | jacket | water resistent black wind breaker | 0.1 |
         * | 109 | spare tire | 24 inch spare tire | 22.2 |
         * | 110 | jacket | new water resistent white wind breaker | 0.5 |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * </pre>
         */

        String[] expected =
                new String[] {
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUniqueIndexIncludingFunction(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // Clear the influence of usesLegacyRows which set USE_LEGACY_TO_STRING = true.
        // In this test, print +I,-U, +U to see more clearly.
        RowUtils.USE_LEGACY_TO_STRING = false;
        initializePostgresTable(POSTGRES_CONTAINER, "index_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE functional_unique_index ("
                                + "    id INTEGER NOT NULL,"
                                + "    char_c STRING"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                // In the snapshot phase of increment snapshot mode, table without
                                // primary key is not allowed now.Thus, when
                                // scan.incremental.snapshot.enabled = true, use 'latest-offset'
                                // startup mode.
                                + (parallelismSnapshot
                                        ? " 'scan.startup.mode' = 'latest-offset',"
                                        : "")
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "indexes",
                        "functional_unique_index",
                        parallelismSnapshot,
                        getSlotName());
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult tableResult = tEnv.executeSql("SELECT * FROM functional_unique_index");
        List<String> expected = new ArrayList<>();
        if (!parallelismSnapshot) {
            expected.add("+I[1, a]");
        }

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000L);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE indexes.functional_unique_index SET char_c=NULL WHERE id=1;");
        }

        expected.addAll(Arrays.asList("-U[1, a]", "+U[1, null]"));
        CloseableIterator<Row> iterator = tableResult.collect();
        assertEqualsInAnyOrder(expected, fetchRows(iterator, expected.size()));
        tableResult.getJobClient().get().cancel().get();
        RowUtils.USE_LEGACY_TO_STRING = true;
    }
}
