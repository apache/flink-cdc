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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;

/** End-to-end tests for Kafka source to values sink pipeline job. */
class KafkaE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaE2eITCase.class);

    private static AdminClient admin;
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
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
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        createTestTopic(1, TOPIC_REPLICATION_FACTOR);
    }

    @AfterEach
    public void after() {
        super.after();
        admin.deleteTopics(Collections.singletonList(topic));
    }

    @Test
    void testSyncCanalJsonFromKafka() throws Exception {
        String groupId = "kafka-source-test-" + UUID.randomUUID();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: kafka\n"
                                + "  topic: %s\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  properties.group.id: %s\n"
                                + "  value.format: canal-json\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: lenient",
                        topic, groupId, parallelism);
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        produceDmlMessages();

        // Verify the CreateTableEvent is emitted with inferred schema and primary key
        waitUntilSpecificEvent(
                "CreateTableEvent{tableId=flink-test.kafka_source_test, "
                        + "schema=columns={`id` INT,`name` STRING,`description` STRING}, "
                        + "primaryKeys=id, options=()}");

        // Verify INSERT events
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=flink-test.kafka_source_test, "
                        + "before=[], after=[1, test1, description1], op=INSERT, meta=()}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=flink-test.kafka_source_test, "
                        + "before=[], after=[2, test2, description2], op=INSERT, meta=()}");

        // Verify UPDATE event
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=flink-test.kafka_source_test, "
                        + "before=[1, test1, description1], "
                        + "after=[1, test1_updated, description1_updated], op=UPDATE, meta=()}");

        // Verify DELETE event
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=flink-test.kafka_source_test, "
                        + "before=[2, test2, description2], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testSyncCanalDdlFromKafka() throws Exception {
        String groupId = "kafka-ddl-test-" + UUID.randomUUID();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: kafka\n"
                                + "  topic: %s\n"
                                + "  properties.bootstrap.servers: kafka:9092\n"
                                + "  properties.group.id: %s\n"
                                + "  value.format: canal-json\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: lenient",
                        topic, groupId, parallelism);
        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        produceDdlMessages();

        // Verify the CreateTableEvent from DDL message
        waitUntilSpecificEvent(
                "CreateTableEvent{tableId=flink-test.users, schema=columns={`id` INT,`name` VARCHAR(100),`age` INT}, primaryKeys=id, options=()}");

        // Verify the AddColumnEvent from ALTER TABLE ADD COLUMN
        waitUntilSpecificEvent(
                "AddColumnEvent{tableId=flink-test.users, "
                        + "addedColumns=[ColumnWithPosition{column=`email` VARCHAR(255), position=LAST, existedColumnName=null}]}");

        // Verify the DropColumnEvent from ALTER TABLE DROP COLUMN
        waitUntilSpecificEvent(
                "DropColumnEvent{tableId=flink-test.users, droppedColumns=[Column{`age`, type=INT}]}");
    }

    /** Produce DML messages (INSERT / UPDATE / DELETE) with primary key. */
    private void produceDmlMessages() throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = buildProducer()) {
            // INSERT id=1 (with pkNames)
            String insertMessage1 =
                    "{\"data\":[{\"id\":1,\"name\":\"test1\",\"description\":\"description1\"}],"
                            + "\"database\":\"flink-test\",\"table\":\"kafka_source_test\","
                            + "\"type\":\"INSERT\",\"pkNames\":[\"id\"],\"isDdl\":false}";
            producer.send(record(insertMessage1)).get();

            // INSERT id=2 (with pkNames)
            String insertMessage2 =
                    "{\"data\":[{\"id\":2,\"name\":\"test2\",\"description\":\"description2\"}],"
                            + "\"database\":\"flink-test\",\"table\":\"kafka_source_test\","
                            + "\"type\":\"INSERT\",\"pkNames\":[\"id\"],\"isDdl\":false}";
            producer.send(record(insertMessage2)).get();

            // UPDATE id=1 (with pkNames)
            String updateMessage =
                    "{\"data\":[{\"id\":1,\"name\":\"test1_updated\",\"description\":\"description1_updated\"}],"
                            + "\"old\":[{\"id\":1,\"name\":\"test1\",\"description\":\"description1\"}],"
                            + "\"database\":\"flink-test\",\"table\":\"kafka_source_test\","
                            + "\"type\":\"UPDATE\",\"pkNames\":[\"id\"],\"isDdl\":false}";
            producer.send(record(updateMessage)).get();

            // DELETE id=2 (with pkNames)
            String deleteMessage =
                    "{\"data\":[{\"id\":2,\"name\":\"test2\",\"description\":\"description2\"}],"
                            + "\"database\":\"flink-test\",\"table\":\"kafka_source_test\","
                            + "\"type\":\"DELETE\",\"pkNames\":[\"id\"],\"isDdl\":false}";
            producer.send(record(deleteMessage)).get();
        }
        LOG.info("DML Canal JSON messages produced to topic: {}", topic);
    }

    /** Produce DDL messages (CREATE TABLE / ALTER TABLE ADD COLUMN / ALTER TABLE DROP COLUMN). */
    private void produceDdlMessages() throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = buildProducer()) {
            // CREATE TABLE with primary key
            String createTableDdl =
                    "{\"isDdl\":true,"
                            + "\"sql\":\"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)\","
                            + "\"database\":\"flink-test\",\"table\":\"users\"}";
            producer.send(record(createTableDdl)).get();

            // ALTER TABLE ADD COLUMN
            String addColumnDdl =
                    "{\"isDdl\":true,"
                            + "\"sql\":\"ALTER TABLE users ADD COLUMN email VARCHAR(255)\","
                            + "\"database\":\"flink-test\",\"table\":\"users\"}";
            producer.send(record(addColumnDdl)).get();

            // ALTER TABLE DROP COLUMN
            String dropColumnDdl =
                    "{\"isDdl\":true,"
                            + "\"sql\":\"ALTER TABLE users DROP COLUMN age\","
                            + "\"database\":\"flink-test\",\"table\":\"users\"}";
            producer.send(record(dropColumnDdl)).get();
        }
        LOG.info("DDL Canal JSON messages produced to topic: {}", topic);
    }

    private KafkaProducer<byte[], byte[]> buildProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        producerProps.put("key.serializer", ByteArraySerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    private ProducerRecord<byte[], byte[]> record(String message) {
        return new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8));
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
