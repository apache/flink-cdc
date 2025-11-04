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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaUtil;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for mysql cdc to Kafka pipeline job. */
class MysqlToKafkaE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlToKafkaE2eITCase.class);

    private static AdminClient admin;
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private TableId table;
    private String topic;
    private KafkaConsumer<byte[], byte[]> consumer;

    @Container
    static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeAll
    static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        createTestTopic(1, TOPIC_REPLICATION_FACTOR);
        Properties properties = getKafkaClientConfiguration();
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        mysqlInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        admin.deleteTopics(Collections.singletonList(topic));
        consumer.close();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabaseWithDebeziumJson() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: kafka\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  topic: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        topic,
                        parallelism);
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        List<ConsumerRecord<byte[], byte[]>> collectedRecords = new ArrayList<>();
        int expectedEventCount = 13;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        List<String> expectedRecords =
                getExpectedRecords("expectedEvents/mysqlToKafka/debezium-json.txt");
        assertThat(expectedRecords).containsAll(deserializeValues(collectedRecords));
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        expectedEventCount = 20;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        assertThat(expectedRecords)
                .containsExactlyInAnyOrderElementsOf(deserializeValues(collectedRecords));
    }

    @Test
    public void testSyncWholeDatabaseWithCanalJson() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: kafka\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  topic: %s\n"
                                + "  value.format: canal-json\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        topic,
                        parallelism);
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        List<ConsumerRecord<byte[], byte[]>> collectedRecords = new ArrayList<>();
        int expectedEventCount = 13;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        List<String> expectedRecords =
                getExpectedRecords("expectedEvents/mysqlToKafka/canal-json.txt");
        assertThat(expectedRecords).containsAll(deserializeValues(collectedRecords));
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        expectedEventCount = 20;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        assertThat(expectedRecords)
                .containsExactlyInAnyOrderElementsOf(deserializeValues(collectedRecords));
    }

    @Test
    public void testSyncWholeDatabaseWithDebeziumJsonHasSchema() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: kafka\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  topic: %s\n"
                                + "  debezium-json.include-schema.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        topic,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, kafkaCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        List<ConsumerRecord<byte[], byte[]>> collectedRecords = new ArrayList<>();
        int expectedEventCount = 13;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        List<String> expectedRecords =
                getExpectedRecords("expectedEvents/mysqlToKafka/debezium-json-with-schema.txt");
        assertThat(expectedRecords).containsAll(deserializeValues(collectedRecords));
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        expectedEventCount = 20;
        waitUntilSpecificEventCount(collectedRecords, expectedEventCount);
        assertThat(expectedRecords)
                .containsExactlyInAnyOrderElementsOf(deserializeValues(collectedRecords));
    }

    private void waitUntilSpecificEventCount(
            List<ConsumerRecord<byte[], byte[]>> actualEvent, int expectedCount) throws Exception {
        boolean result = false;
        long endTimeout =
                System.currentTimeMillis() + MysqlToKafkaE2eITCase.EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < endTimeout) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(actualEvent::add);
            if (actualEvent.size() == expectedCount) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event count: "
                            + expectedCount
                            + " from stdout: "
                            + actualEvent.size());
        }
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return standardProps;
    }

    private void createTestTopic(int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        table =
                TableId.tableId(
                        "default_namespace", "default_schema", UUID.randomUUID().toString());
        topic = table.toString();
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    private List<String> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records)
            throws IOException {
        List<String> result = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            result.add(new String(record.value(), "UTF-8"));
        }
        return result;
    }

    protected List<String> getExpectedRecords(String resourceDirFormat) throws Exception {
        URL url =
                MysqlToKafkaE2eITCase.class
                        .getClassLoader()
                        .getResource(String.format(resourceDirFormat));
        return Files.readAllLines(Paths.get(url.toURI())).stream()
                .filter(this::isValidJsonRecord)
                .map(
                        line ->
                                line.replace(
                                        "$databaseName", mysqlInventoryDatabase.getDatabaseName()))
                .collect(Collectors.toList());
    }

    protected boolean isValidJsonRecord(String line) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.readTree(line);
            return !StringUtils.isEmpty(line);
        } catch (JsonProcessingException e) {
            return false;
        }
    }
}
