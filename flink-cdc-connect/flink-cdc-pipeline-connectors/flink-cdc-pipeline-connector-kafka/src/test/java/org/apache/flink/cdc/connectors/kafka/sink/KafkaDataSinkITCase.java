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
import org.apache.flink.cdc.common.types.DataType;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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

    @ParameterizedTest
    @EnumSource(JsonSerializationType.class)
    void testComplexTypeSerialization(JsonSerializationType serializationType) throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEventsWithComplexTypes(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        if (serializationType == JsonSerializationType.CANAL_JSON) {
            config.put(
                    KafkaDataSinkOptions.VALUE_FORMAT.key(),
                    JsonSerializationType.CANAL_JSON.toString());
        }
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
        assertThat(collectedRecords).hasSize(3);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);

        // Verify complex type serialization with detailed value checks
        verifyComplexTypeRecords(collectedRecords, mapper, serializationType);
        checkProducerLeak();
    }

    @ParameterizedTest
    @EnumSource(JsonSerializationType.class)
    void testNestedArraysSerialization(JsonSerializationType serializationType) throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Create events with nested arrays: ARRAY<ARRAY<STRING>>
        List<Event> events = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn(
                                "nested_arr", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())))
                        .primaryKey("id")
                        .build();
        events.add(new CreateTableEvent(table1, schema));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))));

        // Create nested array: [["a", "b"], ["c", "d"]]
        org.apache.flink.cdc.common.data.GenericArrayData innerArray1 =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("a"), BinaryStringData.fromString("b")
                        });
        org.apache.flink.cdc.common.data.GenericArrayData innerArray2 =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("c"), BinaryStringData.fromString("d")
                        });
        org.apache.flink.cdc.common.data.GenericArrayData outerArray =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {innerArray1, innerArray2});

        events.add(
                DataChangeEvent.insertEvent(
                        table1, generator.generate(new Object[] {1, outerArray})));

        final DataStream<Event> source = env.fromCollection(events, new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        if (serializationType == JsonSerializationType.CANAL_JSON) {
            config.put(
                    KafkaDataSinkOptions.VALUE_FORMAT.key(),
                    JsonSerializationType.CANAL_JSON.toString());
        }
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
        assertThat(collectedRecords).hasSize(1);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);

        JsonNode actual = mapper.readTree(collectedRecords.get(0).value());
        JsonNode dataNode =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual.get("after")
                        : actual.get("data").get(0);

        // Verify nested array structure
        JsonNode nestedArr = dataNode.get("nested_arr");
        assertThat(nestedArr.isArray()).isTrue();
        assertThat(nestedArr.size()).isEqualTo(2);
        assertThat(nestedArr.get(0).isArray()).isTrue();
        assertThat(nestedArr.get(0).size()).isEqualTo(2);
        assertThat(nestedArr.get(0).get(0).asText()).isEqualTo("a");
        assertThat(nestedArr.get(0).get(1).asText()).isEqualTo("b");
        assertThat(nestedArr.get(1).get(0).asText()).isEqualTo("c");
        assertThat(nestedArr.get(1).get(1).asText()).isEqualTo("d");

        checkProducerLeak();
    }

    @ParameterizedTest
    @EnumSource(JsonSerializationType.class)
    void testMapWithArrayValueSerialization(JsonSerializationType serializationType)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Create events with MAP<STRING, ARRAY<INT>>
        List<Event> events = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn(
                                "map_arr",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
                        .primaryKey("id")
                        .build();
        events.add(new CreateTableEvent(table1, schema));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));

        // Create map with array values: {"key1": [1, 2], "key2": [3, 4, 5]}
        Map<Object, Object> mapValues = new HashMap<>();
        mapValues.put(
                BinaryStringData.fromString("key1"),
                new org.apache.flink.cdc.common.data.GenericArrayData(new Object[] {1, 2}));
        mapValues.put(
                BinaryStringData.fromString("key2"),
                new org.apache.flink.cdc.common.data.GenericArrayData(new Object[] {3, 4, 5}));
        org.apache.flink.cdc.common.data.GenericMapData mapData =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues);

        events.add(
                DataChangeEvent.insertEvent(table1, generator.generate(new Object[] {1, mapData})));

        final DataStream<Event> source = env.fromCollection(events, new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        if (serializationType == JsonSerializationType.CANAL_JSON) {
            config.put(
                    KafkaDataSinkOptions.VALUE_FORMAT.key(),
                    JsonSerializationType.CANAL_JSON.toString());
        }
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
        assertThat(collectedRecords).hasSize(1);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);

        JsonNode actual = mapper.readTree(collectedRecords.get(0).value());
        JsonNode dataNode =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual.get("after")
                        : actual.get("data").get(0);

        // Verify map with array values
        JsonNode mapArr = dataNode.get("map_arr");
        assertThat(mapArr.isObject()).isTrue();
        assertThat(mapArr.has("key1")).isTrue();
        assertThat(mapArr.get("key1").isArray()).isTrue();
        assertThat(mapArr.get("key1").size()).isEqualTo(2);
        assertThat(mapArr.get("key1").get(0).asInt()).isEqualTo(1);
        assertThat(mapArr.get("key1").get(1).asInt()).isEqualTo(2);
        assertThat(mapArr.get("key2").size()).isEqualTo(3);
        assertThat(mapArr.get("key2").get(0).asInt()).isEqualTo(3);
        assertThat(mapArr.get("key2").get(1).asInt()).isEqualTo(4);
        assertThat(mapArr.get("key2").get(2).asInt()).isEqualTo(5);

        checkProducerLeak();
    }

    @ParameterizedTest
    @EnumSource(JsonSerializationType.class)
    void testNullAndEmptyComplexTypesSerialization(JsonSerializationType serializationType)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Create events with NULL and empty complex types
        List<Event> events = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .primaryKey("id")
                        .build();
        events.add(new CreateTableEvent(table1, schema));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

        // Event 1: NULL values
        events.add(
                DataChangeEvent.insertEvent(
                        table1, generator.generate(new Object[] {1, null, null})));

        // Event 2: Empty array and map
        org.apache.flink.cdc.common.data.GenericArrayData emptyArray =
                new org.apache.flink.cdc.common.data.GenericArrayData(new Object[] {});
        org.apache.flink.cdc.common.data.GenericMapData emptyMap =
                new org.apache.flink.cdc.common.data.GenericMapData(new HashMap<>());
        events.add(
                DataChangeEvent.insertEvent(
                        table1, generator.generate(new Object[] {2, emptyArray, emptyMap})));

        // Event 3: Array with NULL elements
        org.apache.flink.cdc.common.data.GenericArrayData arrayWithNulls =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("a"), null, BinaryStringData.fromString("b")
                        });
        Map<Object, Object> mapValues = new HashMap<>();
        mapValues.put(BinaryStringData.fromString("key"), 1);
        org.apache.flink.cdc.common.data.GenericMapData mapData =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues);
        events.add(
                DataChangeEvent.insertEvent(
                        table1, generator.generate(new Object[] {3, arrayWithNulls, mapData})));

        final DataStream<Event> source = env.fromCollection(events, new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        if (serializationType == JsonSerializationType.CANAL_JSON) {
            config.put(
                    KafkaDataSinkOptions.VALUE_FORMAT.key(),
                    JsonSerializationType.CANAL_JSON.toString());
        }
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
        assertThat(collectedRecords).hasSize(3);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);

        // Verify event 1: NULL values
        JsonNode actual1 = mapper.readTree(collectedRecords.get(0).value());
        JsonNode dataNode1 =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual1.get("after")
                        : actual1.get("data").get(0);
        assertThat(dataNode1.get("id").asInt()).isEqualTo(1);
        assertThat(dataNode1.get("arr").isNull()).isTrue();
        assertThat(dataNode1.get("map").isNull()).isTrue();

        // Verify event 2: Empty array and map
        JsonNode actual2 = mapper.readTree(collectedRecords.get(1).value());
        JsonNode dataNode2 =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual2.get("after")
                        : actual2.get("data").get(0);
        assertThat(dataNode2.get("id").asInt()).isEqualTo(2);
        assertThat(dataNode2.get("arr").isArray()).isTrue();
        assertThat(dataNode2.get("arr").size()).isEqualTo(0);
        assertThat(dataNode2.get("map").isObject()).isTrue();
        assertThat(dataNode2.get("map").size()).isEqualTo(0);

        // Verify event 3: Array with NULL elements
        JsonNode actual3 = mapper.readTree(collectedRecords.get(2).value());
        JsonNode dataNode3 =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual3.get("after")
                        : actual3.get("data").get(0);
        assertThat(dataNode3.get("id").asInt()).isEqualTo(3);
        assertThat(dataNode3.get("arr").isArray()).isTrue();
        assertThat(dataNode3.get("arr").size()).isEqualTo(3);
        assertThat(dataNode3.get("arr").get(0).asText()).isEqualTo("a");
        assertThat(dataNode3.get("arr").get(1).isNull()).isTrue();
        assertThat(dataNode3.get("arr").get(2).asText()).isEqualTo("b");

        checkProducerLeak();
    }

    @ParameterizedTest
    @EnumSource(JsonSerializationType.class)
    void testDeepNestedStructureSerialization(JsonSerializationType serializationType)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Create events with deep nesting: ARRAY<ROW<name STRING, nested ROW<x INT, y INT>>>
        List<Event> events = new ArrayList<>();
        DataType innerRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("x", DataTypes.INT()),
                        DataTypes.FIELD("y", DataTypes.INT()));
        DataType outerRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("nested", innerRowType));
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("deep", DataTypes.ARRAY(outerRowType))
                        .primaryKey("id")
                        .build();
        events.add(new CreateTableEvent(table1, schema));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(outerRowType)));

        // Create deeply nested structure
        BinaryRecordDataGenerator innerRowGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.INT()));
        org.apache.flink.cdc.common.data.RecordData innerRow =
                innerRowGenerator.generate(new Object[] {10, 20});

        BinaryRecordDataGenerator outerRowGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), innerRowType));
        org.apache.flink.cdc.common.data.RecordData outerRow =
                outerRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("test"), innerRow});

        org.apache.flink.cdc.common.data.GenericArrayData arrayData =
                new org.apache.flink.cdc.common.data.GenericArrayData(new Object[] {outerRow});

        events.add(
                DataChangeEvent.insertEvent(
                        table1, generator.generate(new Object[] {1, arrayData})));

        final DataStream<Event> source = env.fromCollection(events, new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        if (serializationType == JsonSerializationType.CANAL_JSON) {
            config.put(
                    KafkaDataSinkOptions.VALUE_FORMAT.key(),
                    JsonSerializationType.CANAL_JSON.toString());
        }
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
        assertThat(collectedRecords).hasSize(1);
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);

        JsonNode actual = mapper.readTree(collectedRecords.get(0).value());
        JsonNode dataNode =
                serializationType == JsonSerializationType.DEBEZIUM_JSON
                        ? actual.get("after")
                        : actual.get("data").get(0);

        // Verify deep nested structure
        JsonNode deep = dataNode.get("deep");
        assertThat(deep.isArray()).isTrue();
        assertThat(deep.size()).isEqualTo(1);
        JsonNode outerRowNode = deep.get(0);
        assertThat(outerRowNode.get("name").asText()).isEqualTo("test");
        JsonNode nestedNode = outerRowNode.get("nested");
        assertThat(nestedNode.get("x").asInt()).isEqualTo(10);
        assertThat(nestedNode.get("y").asInt()).isEqualTo(20);

        checkProducerLeak();
    }

    private void verifyComplexTypeRecords(
            List<ConsumerRecord<byte[], byte[]>> records,
            ObjectMapper mapper,
            JsonSerializationType serializationType)
            throws IOException {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            JsonNode actual = mapper.readTree(record.value());
            JsonNode dataNode;

            if (serializationType == JsonSerializationType.DEBEZIUM_JSON) {
                assertThat(actual.has("after")).isTrue();
                dataNode = actual.get("after");
            } else {
                assertThat(actual.has("data")).isTrue();
                dataNode = actual.get("data").get(0);
            }

            // Verify id field
            assertThat(dataNode.has("id")).isTrue();
            int id = dataNode.get("id").asInt();
            assertThat(id).isIn(1, 2, 3);

            // Verify ARRAY field with actual values
            assertThat(dataNode.has("arr_col")).isTrue();
            JsonNode arrCol = dataNode.get("arr_col");
            assertThat(arrCol.isArray()).isTrue();
            assertThat(arrCol.size()).isGreaterThan(0);

            // Verify MAP field with actual values
            assertThat(dataNode.has("map_col")).isTrue();
            JsonNode mapCol = dataNode.get("map_col");
            assertThat(mapCol.isObject()).isTrue();
            assertThat(mapCol.size()).isGreaterThan(0);

            // Verify ROW field with nested structure
            assertThat(dataNode.has("row_col")).isTrue();
            JsonNode rowCol = dataNode.get("row_col");
            assertThat(rowCol.isObject()).isTrue();
            assertThat(rowCol.has("name")).isTrue();
            assertThat(rowCol.has("age")).isTrue();
            assertThat(rowCol.get("name").isTextual()).isTrue();
            assertThat(rowCol.get("age").isInt()).isTrue();

            // Verify specific values based on id
            if (id == 1) {
                // arr_col: ["apple", "banana"]
                assertThat(arrCol.size()).isEqualTo(2);
                assertThat(arrCol.get(0).asText()).isEqualTo("apple");
                assertThat(arrCol.get(1).asText()).isEqualTo("banana");

                // map_col: {"count": 10, "total": 100}
                assertThat(mapCol.has("count")).isTrue();
                assertThat(mapCol.get("count").asInt()).isEqualTo(10);
                assertThat(mapCol.has("total")).isTrue();
                assertThat(mapCol.get("total").asInt()).isEqualTo(100);

                // row_col: {"name": "Alice", "age": 30}
                assertThat(rowCol.get("name").asText()).isEqualTo("Alice");
                assertThat(rowCol.get("age").asInt()).isEqualTo(30);
            } else if (id == 2) {
                // arr_col: ["cat", "dog", "bird"]
                assertThat(arrCol.size()).isEqualTo(3);
                assertThat(arrCol.get(0).asText()).isEqualTo("cat");
                assertThat(arrCol.get(1).asText()).isEqualTo("dog");
                assertThat(arrCol.get(2).asText()).isEqualTo("bird");

                // map_col: {"x": 5, "y": 15}
                assertThat(mapCol.has("x")).isTrue();
                assertThat(mapCol.get("x").asInt()).isEqualTo(5);
                assertThat(mapCol.has("y")).isTrue();
                assertThat(mapCol.get("y").asInt()).isEqualTo(15);

                // row_col: {"name": "Bob", "age": 25}
                assertThat(rowCol.get("name").asText()).isEqualTo("Bob");
                assertThat(rowCol.get("age").asInt()).isEqualTo(25);
            } else if (id == 3) {
                // arr_col: ["test"]
                assertThat(arrCol.size()).isEqualTo(1);
                assertThat(arrCol.get(0).asText()).isEqualTo("test");

                // map_col: {"key": 999}
                assertThat(mapCol.has("key")).isTrue();
                assertThat(mapCol.get("key").asInt()).isEqualTo(999);

                // row_col: {"name": "Charlie", "age": 35}
                assertThat(rowCol.get("name").asText()).isEqualTo("Charlie");
                assertThat(rowCol.get("age").asInt()).isEqualTo(35);
            }
        }
    }

    private List<Event> createSourceEventsWithComplexTypes() {
        List<Event> events = new ArrayList<>();
        // create table with complex types
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("arr_col", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn(
                                "map_col", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .physicalColumn(
                                "row_col",
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT())))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        events.add(createTableEvent);

        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("age", DataTypes.INT())));

        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        // Create nested row generator
        BinaryRecordDataGenerator nestedRowGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.INT()));

        // insert event 1
        org.apache.flink.cdc.common.data.GenericArrayData arrayData1 =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("apple"),
                            BinaryStringData.fromString("banana")
                        });
        Map<Object, Object> mapValues1 = new HashMap<>();
        mapValues1.put(BinaryStringData.fromString("count"), 10);
        mapValues1.put(BinaryStringData.fromString("total"), 100);
        org.apache.flink.cdc.common.data.GenericMapData mapData1 =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues1);
        org.apache.flink.cdc.common.data.RecordData nestedRow1 =
                nestedRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("Alice"), 30});

        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(new Object[] {1, arrayData1, mapData1, nestedRow1}));
        events.add(insertEvent1);

        // insert event 2
        org.apache.flink.cdc.common.data.GenericArrayData arrayData2 =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("cat"),
                            BinaryStringData.fromString("dog"),
                            BinaryStringData.fromString("bird")
                        });
        Map<Object, Object> mapValues2 = new HashMap<>();
        mapValues2.put(BinaryStringData.fromString("x"), 5);
        mapValues2.put(BinaryStringData.fromString("y"), 15);
        org.apache.flink.cdc.common.data.GenericMapData mapData2 =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues2);
        org.apache.flink.cdc.common.data.RecordData nestedRow2 =
                nestedRowGenerator.generate(new Object[] {BinaryStringData.fromString("Bob"), 25});

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(new Object[] {2, arrayData2, mapData2, nestedRow2}));
        events.add(insertEvent2);

        // insert event 3 with nested complex types
        org.apache.flink.cdc.common.data.GenericArrayData arrayData3 =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {BinaryStringData.fromString("test")});
        Map<Object, Object> mapValues3 = new HashMap<>();
        mapValues3.put(BinaryStringData.fromString("key"), 999);
        org.apache.flink.cdc.common.data.GenericMapData mapData3 =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues3);
        org.apache.flink.cdc.common.data.RecordData nestedRow3 =
                nestedRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("Charlie"), 35});

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(new Object[] {3, arrayData3, mapData3, nestedRow3}));
        events.add(insertEvent3);

        return events;
    }
}
