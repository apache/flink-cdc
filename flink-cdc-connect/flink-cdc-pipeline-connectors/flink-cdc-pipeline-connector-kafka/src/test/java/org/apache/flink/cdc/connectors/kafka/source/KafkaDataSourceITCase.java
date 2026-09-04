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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationType;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;

import org.apache.flink.cdc.connectors.kafka.sink.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link KafkaDataSource} reading Canal JSON from Kafka. */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class KafkaDataSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataSourceITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();

    public static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeAll
    static void setup() {
        KAFKA_CONTAINER.start();
    }

    @AfterAll
    static void teardown() {
        KAFKA_CONTAINER.stop();
    }

    @Test
    void testReadCanalJsonFromKafka() throws Exception {
        String topic = "test-canal-json-" + UUID.randomUUID();
        String groupId = "test-group-" + UUID.randomUUID();
        String database = "mydb";
        String table = "users";

        // Produce test messages to Kafka
        produceCanalJsonMessages(topic, database, table);

        // Create KafkaDataSource
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(true, null, null, null);

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());

        KafkaDataSource dataSource =
                new KafkaDataSource(
                        topic,
                        kafkaProperties,
                        groupId,
                        StartupMode.EARLIEST_OFFSET,
                        null,
                        null,
                        deserializationSchema);

        // Verify the source can be created
        assertThat(dataSource).isNotNull();
        assertThat(dataSource.getEventSourceProvider()).isInstanceOf(FlinkSourceProvider.class);
        assertThat(dataSource.getMetadataAccessor()).isNotNull();
        assertThat(dataSource.isParallelMetadataSource()).isTrue();
    }

    @Test
    void testKafkaDataSourceWithDefaultTableName() {
        String topic = "test-default-table-" + UUID.randomUUID();
        String groupId = "test-group-" + UUID.randomUUID();

        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(true, "defaultdb", null, "mytable");

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());

        KafkaDataSource dataSource =
                new KafkaDataSource(
                        topic,
                        kafkaProperties,
                        groupId,
                        StartupMode.LATEST_OFFSET,
                        null,
                        null,
                        deserializationSchema);

        assertThat(dataSource).isNotNull();
    }

    @Test
    void testDataSourceFactoryCreation() {
        String topic = "test-factory-" + UUID.randomUUID();
        String groupId = "test-group-" + UUID.randomUUID();

        Map<String, String> config = new HashMap<>();
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(
                KafkaDataSourceOptions.PROPERTIES_PREFIX + "bootstrap.servers",
                KAFKA_CONTAINER.getBootstrapServers());
        config.put(KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id", groupId);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), JsonDeserializationType.CANAL_JSON.getValue());
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "EARLIEST_OFFSET");

        KafkaDataSourceFactory factory = new KafkaDataSourceFactory();
        KafkaDataSource dataSource =
                (KafkaDataSource)
                        factory.createDataSource(
                                new org.apache.flink.cdc.common.factories.FactoryHelper
                                        .DefaultContext(
                                        org.apache.flink.cdc.common.configuration.Configuration
                                                .fromMap(config),
                                        org.apache.flink.cdc.common.configuration.Configuration
                                                .fromMap(new HashMap<>()),
                                        Thread.currentThread().getContextClassLoader()));

        assertThat(dataSource).isNotNull();
    }

    private void produceCanalJsonMessages(String topic, String database, String table) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            // Insert
            String insertJson =
                    String.format(
                            "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":30}],"
                                    + "\"type\":\"INSERT\",\"database\":\"%s\",\"table\":\"%s\","
                                    + "\"pkNames\":[\"id\"]}",
                            database, table);
            producer.send(new ProducerRecord<>(topic, insertJson.getBytes(StandardCharsets.UTF_8)));

            // Update
            String updateJson =
                    String.format(
                            "{\"old\":[{\"id\":1,\"name\":\"Alice\",\"age\":30}],"
                                    + "\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":31}],"
                                    + "\"type\":\"UPDATE\",\"database\":\"%s\",\"table\":\"%s\","
                                    + "\"pkNames\":[\"id\"]}",
                            database, table);
            producer.send(new ProducerRecord<>(topic, updateJson.getBytes(StandardCharsets.UTF_8)));

            // Delete
            String deleteJson =
                    String.format(
                            "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":31}],"
                                    + "\"type\":\"DELETE\",\"database\":\"%s\",\"table\":\"%s\","
                                    + "\"pkNames\":[\"id\"]}",
                            database, table);
            producer.send(new ProducerRecord<>(topic, deleteJson.getBytes(StandardCharsets.UTF_8)));

            producer.flush();
        }
    }
}
