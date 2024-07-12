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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.*;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.flink.cdc.connectors.elasticsearch.v2.NetworkConfig;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase tests for {@link ElasticsearchDataSink}. */
@Testcontainers
public class ElasticsearchDataSinkITCaseTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchDataSinkITCaseTest.class);
    private static final String ELASTICSEARCH_VERSION = "8.12.0";
    private static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse(
                            "docker.elastic.co/elasticsearch/elasticsearch:"
                                    + ELASTICSEARCH_VERSION)
                    .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");

    @Container
    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER =
            createElasticsearchContainer();

    private ElasticsearchClient client;

    @BeforeEach
    public void setUp() {
        client = createElasticsearchClient();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        TableId tableId = TableId.tableId("default", "schema", "table");
        List<Event> events = createTestEvents(tableId);

        runJobWithEvents(events);

        verifyInsertedData(
                tableId,
                "1",
                1,
                1.0,
                "value1",
                true,
                (byte) 1,
                (short) 2,
                100L,
                1.0f,
                new BigDecimal("10.00"),
                1633024800000L);
        verifyInsertedData(
                tableId,
                "2",
                2,
                2.0,
                "value2",
                false,
                (byte) 2,
                (short) 3,
                200L,
                2.0f,
                new BigDecimal("20.00"),
                1633111200000L);
    }

    @Test
    public void testElasticsearchInsertAndDelete() throws Exception {
        TableId tableId = TableId.tableId("default", "schema", "table");
        List<Event> events = createTestEventsWithDelete(tableId);

        runJobWithEvents(events);

        verifyDeletedData(tableId, "2");
    }

    @Test
    public void testElasticsearchAddColumn() throws Exception {
        TableId tableId = TableId.tableId("default", "schema", "table");
        List<Event> events = createTestEventsWithAddColumn(tableId);

        runJobWithEvents(events);

        verifyInsertedDataWithNewColumn(tableId, "3", 3, 3.0, "value3", true);
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        return new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                .withEnv("logger.org.elasticsearch", "ERROR")
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    private ElasticsearchClient createElasticsearchClient() {
        RestClientTransport transport =
                new RestClientTransport(
                        RestClient.builder(
                                        new HttpHost(
                                                ELASTICSEARCH_CONTAINER.getHost(),
                                                ELASTICSEARCH_CONTAINER.getFirstMappedPort(),
                                                "http"))
                                .build(),
                        new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private void runJobWithEvents(List<Event> events) throws Exception {
        ElasticsearchSinkOptions options = createSinkOptions();
        StreamExecutionEnvironment env = createStreamExecutionEnvironment();
        ElasticsearchDataSink<Event> sink =
                new ElasticsearchDataSink<>(options, ZoneId.systemDefault());

        DataStream<Event> stream = env.fromCollection(events, TypeInformation.of(Event.class));
        Sink<Event> elasticsearchSink = ((FlinkSinkProvider) sink.getEventSinkProvider()).getSink();
        stream.sinkTo(elasticsearchSink);

        env.execute("Elasticsearch Sink Test");
    }

    private ElasticsearchSinkOptions createSinkOptions() {
        NetworkConfig networkConfig =
                new NetworkConfig(
                        Collections.singletonList(
                                new HttpHost(
                                        ELASTICSEARCH_CONTAINER.getHost(),
                                        ELASTICSEARCH_CONTAINER.getFirstMappedPort())),
                        null,
                        null,
                        null,
                        null,
                        null);

        return new ElasticsearchSinkOptions(
                5, 1, 10, 50 * 1024 * 1024, 1000, 10 * 1024 * 1024, networkConfig);
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    private void verifyInsertedData(
            TableId tableId,
            String id,
            int expectedId,
            double expectedNumber,
            String expectedName,
            boolean expectedBool,
            byte expectedTinyint,
            short expectedSmallint,
            long expectedBigint,
            float expectedFloat,
            BigDecimal expectedDecimal,
            long expectedTimestamp)
            throws Exception {
        GetRequest getRequest = new GetRequest.Builder().index(tableId.toString()).id(id).build();
        GetResponse<Map> response = client.get(getRequest, Map.class);

        LOG.debug("Response source: {}", response.source());

        assertThat(response.source()).isNotNull();
        assertThat(((Number) response.source().get("id")).intValue()).isEqualTo(expectedId);
        assertThat(((Number) response.source().get("number")).doubleValue())
                .isEqualTo(expectedNumber);
        assertThat(response.source().get("name")).isEqualTo(expectedName);
        assertThat(response.source().get("bool")).isEqualTo(expectedBool);
        assertThat(((Number) response.source().get("tinyint")).byteValue())
                .isEqualTo(expectedTinyint);
        assertThat(((Number) response.source().get("smallint")).shortValue())
                .isEqualTo(expectedSmallint);
        assertThat(((Number) response.source().get("bigint")).longValue())
                .isEqualTo(expectedBigint);
        assertThat(((Number) response.source().get("float")).floatValue()).isEqualTo(expectedFloat);
        assertThat(new BigDecimal(response.source().get("decimal").toString()))
                .isEqualTo(expectedDecimal);

        String timestampString = response.source().get("timestamp").toString();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        LocalDateTime dateTime = LocalDateTime.parse(timestampString, formatter);
        long timestampMillis = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(timestampMillis).isEqualTo(expectedTimestamp);
    }

    private void verifyDeletedData(TableId tableId, String id) throws Exception {
        GetRequest getRequest = new GetRequest.Builder().index(tableId.toString()).id(id).build();
        GetResponse<Map> response = client.get(getRequest, Map.class);

        assertThat(response.source()).isNull();
    }

    private void verifyInsertedDataWithNewColumn(
            TableId tableId,
            String id,
            int expectedId,
            double expectedNumber,
            String expectedName,
            boolean expectedExtraBool)
            throws Exception {
        GetRequest getRequest = new GetRequest.Builder().index(tableId.toString()).id(id).build();
        GetResponse<Map> response = client.get(getRequest, Map.class);

        assertThat(response.source()).isNotNull();
        assertThat(response.source().get("id")).isEqualTo(expectedId);
        assertThat(response.source().get("number")).isEqualTo(expectedNumber);
        assertThat(response.source().get("name")).isEqualTo(expectedName);
        assertThat(response.source().get("extra_bool")).isEqualTo(expectedExtraBool);
    }

    private List<Event> createTestEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .column(new PhysicalColumn("bool", DataTypes.BOOLEAN(), null))
                        .column(new PhysicalColumn("tinyint", DataTypes.TINYINT(), null))
                        .column(new PhysicalColumn("smallint", DataTypes.SMALLINT(), null))
                        .column(new PhysicalColumn("bigint", DataTypes.BIGINT(), null))
                        .column(new PhysicalColumn("float", DataTypes.FLOAT(), null))
                        .column(new PhysicalColumn("decimal", DataTypes.DECIMAL(10, 2), null))
                        .column(new PhysicalColumn("timestamp", DataTypes.TIMESTAMP(), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                org.apache.flink.cdc.common.types.DataTypes.BOOLEAN(),
                                org.apache.flink.cdc.common.types.DataTypes.TINYINT(),
                                org.apache.flink.cdc.common.types.DataTypes.SMALLINT(),
                                org.apache.flink.cdc.common.types.DataTypes.BIGINT(),
                                org.apache.flink.cdc.common.types.DataTypes.FLOAT(),
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(10, 2),
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP()));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1,
                                    1.0,
                                    BinaryStringData.fromString("value1"),
                                    true,
                                    (byte) 1,
                                    (short) 2,
                                    100L,
                                    1.0f,
                                    DecimalData.fromBigDecimal(new BigDecimal("10.00"), 10, 2),
                                    TimestampData.fromMillis(1633024800000L)
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    2,
                                    2.0,
                                    BinaryStringData.fromString("value2"),
                                    false,
                                    (byte) 2,
                                    (short) 3,
                                    200L,
                                    2.0f,
                                    DecimalData.fromBigDecimal(new BigDecimal("20.00"), 10, 2),
                                    TimestampData.fromMillis(1633111200000L)
                                })));
    }

    private List<Event> createTestEventsWithDelete(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING()));

        BinaryRecordData insertRecord1 =
                generator.generate(new Object[] {1, 1.0, BinaryStringData.fromString("value1")});
        BinaryRecordData insertRecord2 =
                generator.generate(new Object[] {2, 2.0, BinaryStringData.fromString("value2")});
        BinaryRecordData deleteRecord =
                generator.generate(new Object[] {2, 2.0, BinaryStringData.fromString("value2")});

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(tableId, insertRecord1),
                DataChangeEvent.insertEvent(tableId, insertRecord2),
                DataChangeEvent.deleteEvent(tableId, deleteRecord));
    }

    private List<Event> createTestEventsWithAddColumn(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                org.apache.flink.cdc.common.types.DataTypes.BOOLEAN()));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_bool", DataTypes.BOOLEAN(), null)))),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    3, 3.0, BinaryStringData.fromString("value3"), true
                                })));
    }
}
