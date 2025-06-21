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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for using {@link KafkaDataSink} writing to a Kafka cluster. */
class KafkaDataSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataSinkITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static AdminClient admin;

    private String topic;

    private TableId table1;

    public static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeAll
    public static void setupAdmin() {
        KAFKA_CONTAINER.start();
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
    }

    @AfterAll
    public static void teardownAdmin() {
        admin.close();
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        table1 =
                TableId.tableId(
                        "default_namespace", "default_schema", UUID.randomUUID().toString());
        topic = table1.toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        deleteTestTopic(topic);
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    private void deleteTestTopic(String topic) throws ExecutionException, InterruptedException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    private List<Event> createSourceEvents() {
        List<Event> events = new ArrayList<>();
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        events.add(createTableEvent);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        events.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        events.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        events.add(insertEvent3);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        events.add(addColumnEvent);

        // rename column
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        events.add(renameColumnEvent);

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("newCol2"));
        events.add(dropColumnEvent);

        // delete
        events.add(
                DataChangeEvent.deleteEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                })));

        // update
        events.add(
                DataChangeEvent.updateEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("x")
                                })));
        return events;
    }

    @Test
    void testDebeziumJsonFormat() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        source.sinkTo(
                ((FlinkSinkProvider)
                                (new KafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .getSink());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, false, 0);
        assertThat(collectedRecords).hasSize(5);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<JsonNode> expected =
                Arrays.asList(
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"1\",\"col2\":\"1\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"2\",\"col2\":\"2\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"3\",\"col2\":\"3\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"1\",\"newCol3\":\"1\"},\"after\":null,\"op\":\"d\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"2\",\"newCol3\":\"\"},\"after\":{\"col1\":\"2\",\"newCol3\":\"x\"},\"op\":\"u\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())));
        assertThat(deserializeValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    @Test
    void testCanalJsonFormat() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(
                KafkaDataSinkOptions.VALUE_FORMAT.key(),
                JsonSerializationType.CANAL_JSON.toString());
        source.sinkTo(
                ((FlinkSinkProvider)
                                (new KafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .getSink());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, false, 0);
        assertThat(collectedRecords).hasSize(5);
        for (ConsumerRecord<byte[], byte[]> consumerRecord : collectedRecords) {
            assertThat(
                            consumerRecord
                                    .headers()
                                    .headers(
                                            PipelineKafkaRecordSerializationSchema
                                                    .TABLE_NAME_HEADER_KEY)
                                    .iterator())
                    .isExhausted();
        }
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<JsonNode> expected =
                Arrays.asList(
                        mapper.readTree(
                                String.format(
                                        "{\"old\":null,\"data\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"old\":null,\"data\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"old\":null,\"data\":[{\"col1\":\"3\",\"col2\":\"3\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"old\":null,\"data\":[{\"col1\":\"1\",\"newCol3\":\"1\"}],\"type\":\"DELETE\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"old\":[{\"col1\":\"2\",\"newCol3\":\"\"}],\"data\":[{\"col1\":\"2\",\"newCol3\":\"x\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                        table1.getTableName())));
        assertThat(deserializeValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    @Test
    void testHashByKeyPartitionStrategyUsingJson() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSinkOptions.KEY_FORMAT.key(), KeyFormat.JSON.toString());
        config.put(
                KafkaDataSinkOptions.VALUE_FORMAT.key(),
                JsonSerializationType.CANAL_JSON.toString());
        source.sinkTo(
                ((FlinkSinkProvider)
                                (new KafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .getSink());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, false);
        assertThat(collectedRecords).hasSize(5);
        for (ConsumerRecord<byte[], byte[]> consumerRecord : collectedRecords) {
            assertThat(
                            consumerRecord
                                    .headers()
                                    .headers(
                                            PipelineKafkaRecordSerializationSchema
                                                    .TABLE_NAME_HEADER_KEY)
                                    .iterator())
                    .isExhausted();
        }
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<Tuple2<JsonNode, JsonNode>> expected =
                Arrays.asList(
                        Tuple2.of(
                                mapper.readTree(
                                        String.format(
                                                "{\"TableId\":\"%s\",\"col1\":\"1\"}",
                                                table1.toString())),
                                mapper.readTree(
                                        String.format(
                                                "{\"old\":null,\"data\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]})",
                                                table1.getTableName()))),
                        Tuple2.of(
                                mapper.readTree(
                                        String.format(
                                                "{\"TableId\":\"%s\",\"col1\":\"2\"}",
                                                table1.toString())),
                                mapper.readTree(
                                        String.format(
                                                "{\"old\":null,\"data\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]})",
                                                table1.getTableName()))),
                        Tuple2.of(
                                mapper.readTree(
                                        String.format(
                                                "{\"TableId\":\"%s\",\"col1\":\"3\"}",
                                                table1.toString())),
                                mapper.readTree(
                                        String.format(
                                                "{\"old\":null,\"data\":[{\"col1\":\"3\",\"col2\":\"3\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]})",
                                                table1.getTableName()))),
                        Tuple2.of(
                                mapper.readTree(
                                        String.format(
                                                "{\"TableId\":\"%s\",\"col1\":\"1\"}",
                                                table1.toString())),
                                mapper.readTree(
                                        String.format(
                                                "{\"old\":null,\"data\":[{\"col1\":\"1\",\"newCol3\":\"1\"}],\"type\":\"DELETE\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]})",
                                                table1.getTableName()))),
                        Tuple2.of(
                                mapper.readTree(
                                        String.format(
                                                "{\"TableId\":\"%s\",\"col1\":\"2\"}",
                                                table1.toString())),
                                mapper.readTree(
                                        String.format(
                                                "{\"old\":[{\"col1\":\"2\",\"newCol3\":\"\"}],\"data\":[{\"col1\":\"2\",\"newCol3\":\"x\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"%s\",\"pkNames\":[\"col1\"]}",
                                                table1.getTableName()))));
        assertThat(deserializeKeyValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    @Test
    void testTopicAndHeaderOption() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        config.put(KafkaDataSinkOptions.TOPIC.key(), "test_topic");
        config.put(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED.key(), "true");
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        source.sinkTo(
                ((FlinkSinkProvider)
                                (new KafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .getSink());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic("test_topic", false, 0);
        assertThat(collectedRecords).hasSize(5);
        for (ConsumerRecord<byte[], byte[]> consumerRecord : collectedRecords) {
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .NAMESPACE_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getNamespace());
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .SCHEMA_NAME_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getSchemaName());
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .TABLE_NAME_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getTableName());
        }
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<JsonNode> expected =
                Arrays.asList(
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"1\",\"col2\":\"1\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"2\",\"col2\":\"2\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"3\",\"col2\":\"3\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"1\",\"newCol3\":\"1\"},\"after\":null,\"op\":\"d\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"2\",\"newCol3\":\"\"},\"after\":{\"col1\":\"2\",\"newCol3\":\"x\"},\"op\":\"u\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())));
        assertThat(deserializeValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    @Test
    void testSinkTableMapping() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source = env.fromData(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        config.put(
                KafkaDataSinkOptions.SINK_TABLE_ID_TO_TOPIC_MAPPING.key(),
                "default_namespace.default_schema_copy.\\.*:test_topic_mapping_copy;default_namespace.default_schema.\\.*:test_topic_mapping");
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        source.sinkTo(
                ((FlinkSinkProvider)
                                (new KafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .getSink());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic("test_topic_mapping", false, 0);
        final long recordsCount = 5;
        assertThat(recordsCount).isEqualTo(collectedRecords.size());
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<JsonNode> expected =
                Arrays.asList(
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"1\",\"col2\":\"1\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"2\",\"col2\":\"2\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":null,\"after\":{\"col1\":\"3\",\"col2\":\"3\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"1\",\"newCol3\":\"1\"},\"after\":null,\"op\":\"d\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())),
                        mapper.readTree(
                                String.format(
                                        "{\"before\":{\"col1\":\"2\",\"newCol3\":\"\"},\"after\":{\"col1\":\"2\",\"newCol3\":\"x\"},\"op\":\"u\",\"source\":{\"db\":\"default_schema\",\"table\":\"%s\"}}",
                                        table1.getTableName())));
        assertThat(deserializeValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, boolean committed, int... partitionArr) {
        Properties properties = getKafkaClientConfiguration();
        Set<Integer> partitions = new HashSet<>();
        for (int partition : partitionArr) {
            partitions.add(partition);
        }
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed, partitions);
    }

    private void checkProducerLeak() throws InterruptedException {
        List<Map.Entry<Thread, StackTraceElement[]>> leaks = null;
        for (int tries = 0; tries < 10; tries++) {
            leaks =
                    Thread.getAllStackTraces().entrySet().stream()
                            .filter(this::findAliveKafkaThread)
                            .collect(Collectors.toList());
            if (leaks.isEmpty()) {
                return;
            }
            Thread.sleep(1000);
        }

        for (Map.Entry<Thread, StackTraceElement[]> leak : leaks) {
            leak.getKey().stop();
        }
        fail(
                "Detected producer leaks:\n"
                        + leaks.stream().map(this::format).collect(Collectors.joining("\n\n")));
    }

    private static List<Tuple2<JsonNode, JsonNode>> deserializeKeyValues(
            List<ConsumerRecord<byte[], byte[]>> records) throws IOException {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<Tuple2<JsonNode, JsonNode>> result = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            result.add(Tuple2.of(mapper.readTree(record.key()), mapper.readTree(record.value())));
        }
        return result;
    }

    private static List<JsonNode> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records)
            throws IOException {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<JsonNode> result = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            result.add(mapper.readTree(record.value()));
        }
        return result;
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
        return standardProps;
    }

    private String format(Map.Entry<Thread, StackTraceElement[]> leak) {
        String stackTrace =
                Arrays.stream(leak.getValue())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n"));
        return leak.getKey().getName() + ":\n" + stackTrace;
    }

    private boolean findAliveKafkaThread(Map.Entry<Thread, StackTraceElement[]> threadStackTrace) {
        return threadStackTrace.getKey().getState() != Thread.State.TERMINATED
                && threadStackTrace.getKey().getName().contains("kafka-producer-network-thread");
    }
}
