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

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaUtil;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for Kafka to MySQL pipeline job. */
class KafkaToMysqlE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToMysqlE2eITCase.class);

    private static AdminClient admin;
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private String topic;

    @Container
    static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeAll
    static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                "bootstrap.servers",
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        createTestTopic(1, TOPIC_REPLICATION_FACTOR);
        createTargetTable();
    }

    @AfterEach
    public void after() {
        super.after();
        admin.deleteTopics(Collections.singletonList(topic));
        dropTargetTable();
    }

    @Test
    void testSyncCanalJsonFromKafkaToMysql() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: kafka\n"
                                + "  topic: %s\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  value.format: canal-json\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  database-name: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        topic,
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        "flink-test",
                        parallelism);
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        produceCanalJsonMessages();

        waitUntilRowCount(3);

        verifyDataInMysql();
    }

    private void produceCanalJsonMessages() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        producerProps.put("key.serializer", ByteArraySerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            String insertMessage =
                    "{\"data\":[{\"id\":1,\"name\":\"test1\",\"description\":\"description1\"}],\"database\":\"flink-test\",\"table\":\"kafka_source_test\",\"type\":\"INSERT\",\"isDdl\":false}";
            producer.send(new ProducerRecord<>(topic, insertMessage.getBytes("UTF-8"))).get();

            String insertMessage2 =
                    "{\"data\":[{\"id\":2,\"name\":\"test2\",\"description\":\"description2\"}],\"database\":\"flink-test\",\"table\":\"kafka_source_test\",\"type\":\"INSERT\",\"isDdl\":false}";
            producer.send(new ProducerRecord<>(topic, insertMessage2.getBytes("UTF-8"))).get();

            String updateMessage =
                    "{\"data\":[{\"id\":1,\"name\":\"test1_updated\",\"description\":\"description1_updated\"}],\"old\":[{\"name\":\"test1\",\"description\":\"description1\"}],\"database\":\"flink-test\",\"table\":\"kafka_source_test\",\"type\":\"UPDATE\",\"isDdl\":false}";
            producer.send(new ProducerRecord<>(topic, updateMessage.getBytes("UTF-8"))).get();
        }
    }

    private void waitUntilRowCount(int expectedCount) throws Exception {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/flink-test", MYSQL.getHost(), MYSQL.getDatabasePort());
        long endTimeout = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < endTimeout) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                 Statement stat = conn.createStatement();
                 ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM kafka_source_test")) {
                if (rs.next() && rs.getInt(1) >= expectedCount) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Timeout waiting for row count: " + expectedCount);
    }

    private void verifyDataInMysql() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/flink-test", MYSQL.getHost(), MYSQL.getDatabasePort());
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
             Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("SELECT id, name, description FROM kafka_source_test ORDER BY id")) {

            rs.next();
            assertThat(rs.getInt("id")).isEqualTo(1);
            assertThat(rs.getString("name")).isEqualTo("test1_updated");
            assertThat(rs.getString("description")).isEqualTo("description1_updated");

            rs.next();
            assertThat(rs.getInt("id")).isEqualTo(2);
            assertThat(rs.getString("name")).isEqualTo("test2");
            assertThat(rs.getString("description")).isEqualTo("description2");
        }
    }

    private void createTargetTable() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/flink-test", MYSQL.getHost(), MYSQL.getDatabasePort());
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
             Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE IF EXISTS kafka_source_test");
            stat.execute("CREATE TABLE kafka_source_test (id INT PRIMARY KEY, name VARCHAR(100), description VARCHAR(255))");
        }
    }

    private void dropTargetTable() {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/flink-test", MYSQL.getHost(), MYSQL.getDatabasePort());
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
             Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE IF EXISTS kafka_source_test");
        } catch (SQLException e) {
            LOG.warn("Failed to drop table", e);
        }
    }

    private void createTestTopic(int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        topic = "kafka-source-test-" + UUID.randomUUID();
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }
}