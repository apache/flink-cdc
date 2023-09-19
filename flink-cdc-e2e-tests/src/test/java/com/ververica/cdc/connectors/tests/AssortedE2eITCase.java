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

package com.ververica.cdc.connectors.tests;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer;
import com.ververica.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;
import com.ververica.cdc.connectors.tests.utils.JdbcProxy;
import com.ververica.cdc.connectors.tests.utils.TestUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.base.utils.EnvironmentUtils.supportCheckpointsAfterTasksFinished;
import static com.ververica.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.MONGODB_PORT;
import static org.junit.Assert.assertNotNull;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** End-to-end tests for combination of cdc connectors uber jar. */
public class AssortedE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(AssortedE2eITCase.class);
    private static final String INTER_CONTAINER_MONGO_ALIAS = "mongodb";

    private static final Path postgresCdcJar = TestUtils.getResource("postgres-cdc-connector.jar");
    private static final Path mongoCdcJar = TestUtils.getResource("mongodb-cdc-connector.jar");
    private static final Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    // MongoDB properties
    private MongoDBContainer container;

    private MongoClient mongoClient;

    // Postgres properties
    private static final String PG_TEST_USER = "postgres";
    private static final String PG_TEST_PASSWORD = "postgres";
    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String INTER_CONTAINER_PG_ALIAS = "postgres";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:9.6").asCompatibleSubstituteFor("postgres");

    @ClassRule
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername(PG_TEST_USER)
                    .withPassword(PG_TEST_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_PG_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_wal_senders=20",
                            "-c",
                            "max_replication_slots=20")
                    .withReuse(true);

    private static final String FLINK_PROPERTIES =
            String.join(
                    "\n",
                    Arrays.asList(
                            "jobmanager.rpc.address: jobmanager",
                            "taskmanager.numberOfTaskSlots: 1",
                            "parallelism.default: 1",
                            "execution.checkpointing.interval: 10000"));

    // MongoDB methods
    private Document productDocOf(String id, String name, String description, Double weight) {
        Document document = new Document();
        if (id != null) {
            document.put("_id", new ObjectId(id));
        }
        document.put("name", name);
        document.put("description", description);
        document.put("weight", weight);
        return document;
    }

    // Postgres methods

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private void initializePostgresTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresE2eITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            Class.forName(PG_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (Connection connection = getPgJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    public static String getSlotName(String prefix) {
        final Random random = new Random();
        int id = random.nextInt(9000);
        return prefix + id;
    }

    // assorted tests
    @Before
    public void before() {
        overrideFlinkProperties(FLINK_PROPERTIES);
        super.before();

        container =
                new MongoDBContainer("mongo:6.0.9")
                        .withSharding()
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_MONGO_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withStartupTimeout(Duration.ofSeconds(120));

        Startables.deepStart(Stream.of(container)).join();

        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(container.getConnectionString()))
                        .build();
        mongoClient = MongoClients.create(settings);

        initializePostgresTable("postgres_inventory");
    }

    @After
    public void after() {
        super.after();
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @Test
    public void testAssortedCDC() throws Exception {
        String dbName =
                container.executeCommandFileInDatabase(
                        "mongo_inventory",
                        "inventory" + Integer.toUnsignedString(new Random().nextInt(), 36));

        container.executeCommandInDatabase(
                "db.runCommand({ collMod: 'products', changeStreamPreAndPostImages: { enabled: true } })",
                dbName);

        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            // gather the initial statistics of the table for splitting
            statement.execute("ANALYZE;");
        }

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "SET 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';",
                        "CREATE TABLE products_source_mysql (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " point_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'mysql-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_MYSQL_ALIAS + "',",
                        " 'port' = '3306',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "',",
                        " 'database-name' = '" + mysqlInventoryDatabase.getDatabaseName() + "',",
                        " 'table-name' = 'products_source',",
                        " 'server-time-zone' = 'UTC',",
                        " 'server-id' = '5800-5900',",
                        " 'scan.incremental.snapshot.chunk.size' = '4',",
                        " 'scan.incremental.close-idle-reader.enabled' = '"
                                + supportCheckpointsAfterTasksFinished()
                                + "'",
                        ");",
                        "CREATE TABLE products_source_mongo (",
                        " _id STRING NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (_id) not enforced",
                        ") WITH (",
                        " 'connector' = 'mongodb-cdc',",
                        " 'connection.options' = 'connectTimeoutMS=12000&socketTimeoutMS=13000',",
                        " 'hosts' = '" + INTER_CONTAINER_MONGO_ALIAS + ":" + MONGODB_PORT + "',",
                        " 'database' = '" + dbName + "',",
                        " 'username' = '" + FLINK_USER + "',",
                        " 'password' = '" + FLINK_USER_PASSWORD + "',",
                        " 'collection' = 'products',",
                        " 'heartbeat.interval.ms' = '1000',",
                        " 'scan.incremental.snapshot.enabled' = 'false',",
                        " 'scan.full-changelog' = 'false',",
                        " 'scan.incremental.close-idle-reader.enabled' = '"
                                + supportCheckpointsAfterTasksFinished()
                                + "'",
                        ");",
                        "CREATE TABLE products_source_pg (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'postgres-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_PG_ALIAS + "',",
                        " 'port' = '" + POSTGRESQL_PORT + "',",
                        " 'username' = '" + PG_TEST_USER + "',",
                        " 'password' = '" + PG_TEST_PASSWORD + "',",
                        " 'database-name' = '" + POSTGRES.getDatabaseName() + "',",
                        " 'schema-name' = 'inventory',",
                        " 'table-name' = 'products',",
                        " 'slot.name' = '" + getSlotName("flink_incremental_") + "',",
                        " 'scan.incremental.snapshot.chunk.size' = '4',",
                        " 'scan.incremental.snapshot.enabled' = 'true',",
                        " 'scan.startup.mode' = 'initial'",
                        ");",
                        "CREATE TABLE products_sink (",
                        " `id` STRING NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = 'mongodb_products_sink',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT CONCAT('mysql_', CAST(`id` AS STRING)), name, description, weight FROM products_source_mysql",
                        "UNION SELECT CONCAT('mongo_', _id), name, description, weight FROM products_source_mongo",
                        "UNION SELECT CONCAT('pg_', CAST(`id` AS STRING)), name, description, weight FROM products_source_pg;");

        submitSQLJob(sqlLines, mongoCdcJar, mysqlCdcJar, postgresCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        // wait a bit to make sure the replication slot is ready
        Thread.sleep(30000);

        // generate mongo binlogs
        MongoCollection<Document> products =
                mongoClient.getDatabase(dbName).getCollection("products");
        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000106")),
                Updates.set("description", "18oz carpenter hammer"));
        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000107")),
                Updates.set("weight", 5.1));
        products.insertOne(
                productDocOf(
                        "100000000000000000000110",
                        "jacket",
                        "water resistent white wind breaker",
                        0.2));
        products.insertOne(
                productDocOf("100000000000000000000111", "scooter", "Big 2-wheel scooter", 5.18));
        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000110")),
                Updates.combine(
                        Updates.set("description", "new water resistent white wind breaker"),
                        Updates.set("weight", 0.5)));
        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000111")),
                Updates.set("weight", 5.17));
        products.deleteOne(Filters.eq("_id", new ObjectId("100000000000000000000111")));

        // generate mysql binlogs
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE products_source SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products_source SET weight='5.1' WHERE id=107;");
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null);");
            stat.execute(
                    "UPDATE products_source SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products_source SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products_source WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // generate postgres WALs
        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {

            // at this point, the replication slot 'flink' should already be created; otherwise, the
            // test will fail
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
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // assert final results
        String mysqlUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        JdbcProxy proxy =
                new JdbcProxy(mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "mysql_101,scooter,Small 2-wheel scooter,3.14",
                        "mysql_102,car battery,12V car battery,8.1",
                        "mysql_103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "mysql_104,hammer,12oz carpenter's hammer,0.75",
                        "mysql_105,hammer,14oz carpenter's hammer,0.875",
                        "mysql_106,hammer,18oz carpenter hammer,1.0",
                        "mysql_107,rocks,box of assorted rocks,5.1",
                        "mysql_108,jacket,water resistent black wind breaker,0.1",
                        "mysql_109,spare tire,24 inch spare tire,22.2",
                        "mysql_110,jacket,new water resistent white wind breaker,0.5",
                        "pg_101,scooter,Small 2-wheel scooter,3.14",
                        "pg_102,car battery,12V car battery,8.1",
                        "pg_103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "pg_104,hammer,12oz carpenter's hammer,0.75",
                        "pg_105,hammer,14oz carpenter's hammer,0.875",
                        "pg_106,hammer,18oz carpenter hammer,1.0",
                        "pg_107,rocks,box of assorted rocks,5.1",
                        "pg_108,jacket,water resistent black wind breaker,0.1",
                        "pg_109,spare tire,24 inch spare tire,22.2",
                        "pg_110,jacket,new water resistent white wind breaker,0.5",
                        "mongo_100000000000000000000101,scooter,Small 2-wheel scooter,3.14",
                        "mongo_100000000000000000000102,car battery,12V car battery,8.1",
                        "mongo_100000000000000000000103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "mongo_100000000000000000000104,hammer,12oz carpenter's hammer,0.75",
                        "mongo_100000000000000000000105,hammer,14oz carpenter's hammer,0.875",
                        "mongo_100000000000000000000106,hammer,18oz carpenter hammer,1.0",
                        "mongo_100000000000000000000107,rocks,box of assorted rocks,5.1",
                        "mongo_100000000000000000000108,jacket,water resistent black wind breaker,0.1",
                        "mongo_100000000000000000000109,spare tire,24 inch spare tire,22.2",
                        "mongo_100000000000000000000110,jacket,new water resistent white wind breaker,0.5");
        proxy.checkResultWithTimeout(
                expectResult,
                "mongodb_products_sink",
                new String[] {"id", "name", "description", "weight"},
                150000L);
    }
}
