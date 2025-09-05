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

package org.apache.flink.cdc.connectors.tests;

import org.apache.flink.cdc.common.test.utils.JdbcProxy;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.supportCheckpointsAfterTasksFinished;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.MONGODB_PORT;

/** End-to-end tests for mongodb-cdc connector uber jar. */
class MongoE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MongoE2eITCase.class);
    private static final String INTER_CONTAINER_MONGO_ALIAS = "mongodb";

    private static final Path mongoCdcJar = TestUtils.getResource("mongodb-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    private MongoDBContainer container;

    private MongoClient mongoClient;

    public static String getMongoVersion() {
        String specifiedMongoVersion = System.getProperty("specifiedMongoVersion");
        if (Objects.isNull(specifiedMongoVersion)) {
            throw new IllegalArgumentException(
                    "No MongoDB version specified to run this test. Please use -DspecifiedMongoVersion to pass one.");
        }
        return specifiedMongoVersion;
    }

    void setup(String mongoVersion, boolean parallelismSnapshot, boolean scanFullChangelog) {
        container =
                new MongoDBContainer("mongo:" + mongoVersion)
                        .withSharding()
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_MONGO_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withStartupTimeout(Duration.ofSeconds(120));

        Startables.deepStart(Stream.of(container)).join();

        if (scanFullChangelog) {
            container.executeCommand(
                    "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");
        }

        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(container.getConnectionString()))
                        .build();
        mongoClient = MongoClients.create(settings);
    }

    @AfterEach
    public void after() {
        super.after();
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @ParameterizedTest(
            name = "MongoDB Version: {0}, boolean parallelismSnapshot: {1}, scanFullChangelog: {2}")
    @CsvSource({
        "6.0.16, true, true",
        "6.0.16, true, false",
        "6.0.16, false, true",
        "6.0.16, false, false",
        "7.0.12, true, true",
        "7.0.12, true, false",
        "7.0.12, false, true",
        "7.0.12, false, false"
    })
    void testMongoDbCDC(String mongoVersion, boolean parallelismSnapshot, boolean scanFullChangelog)
            throws Exception {
        setup(mongoVersion, parallelismSnapshot, scanFullChangelog);
        String dbName =
                container.executeCommandFileInDatabase(
                        "mongo_inventory",
                        "inventory" + Integer.toUnsignedString(new Random().nextInt(), 36));

        container.executeCommandInDatabase(
                "db.runCommand({ collMod: 'products', changeStreamPreAndPostImages: { enabled: true } })",
                dbName);
        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "SET 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';",
                        "CREATE TABLE products_source (",
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
                        " 'scan.incremental.snapshot.enabled' = '" + parallelismSnapshot + "',",
                        " 'scan.full-changelog' = '" + scanFullChangelog + "',",
                        " 'scan.incremental.close-idle-reader.enabled' = '"
                                + supportCheckpointsAfterTasksFinished()
                                + "'",
                        ");",
                        "CREATE TABLE mongodb_products_sink (",
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
                        "INSERT INTO mongodb_products_sink",
                        "SELECT * FROM products_source;");

        submitSQLJob(sqlLines, mongoCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
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
                        "100000000000000000000101,scooter,Small 2-wheel scooter,3.14",
                        "100000000000000000000102,car battery,12V car battery,8.1",
                        "100000000000000000000103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "100000000000000000000104,hammer,12oz carpenter's hammer,0.75",
                        "100000000000000000000105,hammer,14oz carpenter's hammer,0.875",
                        "100000000000000000000106,hammer,18oz carpenter hammer,1.0",
                        "100000000000000000000107,rocks,box of assorted rocks,5.1",
                        "100000000000000000000108,jacket,water resistent black wind breaker,0.1",
                        "100000000000000000000109,spare tire,24 inch spare tire,22.2",
                        "100000000000000000000110,jacket,new water resistent white wind breaker,0.5");
        proxy.checkResultWithTimeout(
                expectResult,
                "mongodb_products_sink",
                new String[] {"id", "name", "description", "weight"},
                150000L);
    }

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
}
