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
import org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.get.GetRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.get.GetResponse;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.RequestOptions;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.RestClient;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.RestHighLevelClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase tests for {@link ElasticsearchDataSink}. */
@Testcontainers
public class Elasticsearch6DataSinkITCaseTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchDataSinkITCaseTest.class);
    private static final String ELASTICSEARCH_VERSION = "7.10.2";
    private static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse(
                            "docker.elastic.co/elasticsearch/elasticsearch:"
                                    + ELASTICSEARCH_VERSION)
                    .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");

    @Container
    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER =
            createElasticsearchContainer();

    private RestHighLevelClient client;

    @BeforeEach
    public void setUp() {
        client = createElasticsearchClient();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
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

    private RestHighLevelClient createElasticsearchClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                ELASTICSEARCH_CONTAINER.getHost(),
                                ELASTICSEARCH_CONTAINER.getFirstMappedPort(),
                                "http")));
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
                                new org.apache.http.HttpHost(
                                        ELASTICSEARCH_CONTAINER.getHost(),
                                        ELASTICSEARCH_CONTAINER.getFirstMappedPort())),
                        null,
                        null,
                        null,
                        null,
                        null);

        return new ElasticsearchSinkOptions(
                5, 1, 10, 50 * 1024 * 1024, 1000, 10 * 1024 * 1024, networkConfig, 7, null, null);
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
        GetRequest getRequest = new GetRequest(tableId.toString()).id(id);

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        LOG.debug("Response source: {}", response.getSource());

        assertThat(response.getSource()).isNotNull();
        assertThat(((Number) response.getSource().get("id")).intValue()).isEqualTo(expectedId);
        assertThat(((Number) response.getSource().get("number")).doubleValue())
                .isEqualTo(expectedNumber);
        assertThat(response.getSource().get("name")).isEqualTo(expectedName);
        assertThat(response.getSource().get("bool")).isEqualTo(expectedBool);
        assertThat(((Number) response.getSource().get("tinyint")).byteValue())
                .isEqualTo(expectedTinyint);
        assertThat(((Number) response.getSource().get("smallint")).shortValue())
                .isEqualTo(expectedSmallint);
        assertThat(((Number) response.getSource().get("bigint")).longValue())
                .isEqualTo(expectedBigint);
        assertThat(((Number) response.getSource().get("float")).floatValue())
                .isEqualTo(expectedFloat);
        assertThat(new BigDecimal(response.getSource().get("decimal").toString()))
                .isEqualTo(expectedDecimal);

        String timestampString = response.getSource().get("timestamp").toString();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        LocalDateTime dateTime = LocalDateTime.parse(timestampString, formatter);
        long timestampMillis = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(timestampMillis).isEqualTo(expectedTimestamp);
    }

    private void verifyDeletedData(TableId tableId, String id) throws Exception {
        GetRequest getRequest = new GetRequest(tableId.toString()).id(id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        assertThat(response.isExists()).isFalse();
    }

    private void verifyInsertedDataWithNewColumn(
            TableId tableId,
            String id,
            int expectedId,
            double expectedNumber,
            String expectedName,
            boolean expectedExtraBool)
            throws Exception {
        GetRequest getRequest = new GetRequest(tableId.toString()).id(id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        assertThat(response.getSource()).isNotNull();
        assertThat(response.getSource().get("id")).isEqualTo(expectedId);
        assertThat(response.getSource().get("number")).isEqualTo(expectedNumber);
        assertThat(response.getSource().get("name")).isEqualTo(expectedName);
        assertThat(response.getSource().get("extra_bool")).isEqualTo(expectedExtraBool);
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
