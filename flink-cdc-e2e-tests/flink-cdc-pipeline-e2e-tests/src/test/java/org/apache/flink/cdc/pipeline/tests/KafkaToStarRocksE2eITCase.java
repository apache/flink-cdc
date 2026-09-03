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
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;

/** End-to-end tests for multi-partition Kafka Debezium and Canal JSON to StarRocks pipelines. */
@EnabledIfSystemProperty(named = "specifiedFlinkVersion", matches = "^1.*")
class KafkaToStarRocksE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToStarRocksE2eITCase.class);

    private static final String DATABASE = "inventory";
    private static final String KAFKA_ALIAS = "kafka";
    private static final String STARROCKS_ALIAS = "starrocks";

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(KAFKA_ALIAS);

    @Container
    private static final StarRocksContainer STARROCKS_CONTAINER =
            new StarRocksContainer(NETWORK).withNetworkAliases(STARROCKS_ALIAS);

    private AdminClient admin;
    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private String table;
    private EventFormat eventFormat;

    @BeforeAll
    public static void initializeContainers() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(KAFKA_CONTAINER, STARROCKS_CONTAINER)).join();
        STARROCKS_CONTAINER.waitForLog(
                ".*Enjoy the journey to StarRocks blazing-fast lake-house engine!.*\\s", 1, 240);
        waitForStarRocksBackend();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        topic = "kafka-customers-" + UUID.randomUUID();
        table = "customers_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        Properties properties = kafkaProperties();
        admin = AdminClient.create(properties);
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, (short) 1)))
                .all()
                .get();
        properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
        properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @AfterEach
    public void after() {
        if (producer != null) {
            producer.close();
        }
        if (admin != null) {
            admin.deleteTopics(Collections.singletonList(topic));
            admin.close();
        }
        dropTableQuietly();
        super.after();
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testCreateTableAddColumnAndModifyColumnEvents(EventFormat format) throws Exception {
        submitKafkaToStarRocksJob(format);

        LOG.info("Test Schema Change - Create Table...");
        send(0, value(createFields(), "{\"id\":1,\"name\":\"alice\",\"age\":18}"));
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "age | int | YES | false | null"));
        validateSinkResult(3, Collections.singletonList("1 | alice | 18"));

        LOG.info("Test Schema Change - Add Column...");
        send(
                0,
                value(
                        addColumnFields(),
                        "{\"id\":2,\"name\":\"bob\",\"age\":21,\"email\":\"bob@example.com\"}"));
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "age | int | YES | false | null",
                        "email | varchar(1048576) | YES | false | null"));
        waitUntilStarRocksSchemaChangeIdle();
        validateSinkResult(
                4, Arrays.asList("1 | alice | 18 | null", "2 | bob | 21 | bob@example.com"));

        LOG.info("Test Schema Change - Alter Column Type...");
        send(
                0,
                value(
                        modifyColumnFields(),
                        "{\"id\":3,\"name\":\"charlie\",\"age\":40,\"email\":\"charlie@example.com\"}"));
        waitUntilStarRocksSchemaChangeIdle();
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "age | bigint | YES | false | null",
                        "email | varchar(1048576) | YES | false | null"));
        validateSinkResult(
                4,
                Arrays.asList(
                        "1 | alice | 18 | null",
                        "2 | bob | 21 | bob@example.com",
                        "3 | charlie | 40 | charlie@example.com"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testNewSchemaThenHistoricalSchemaFromAnotherPartition(EventFormat format)
            throws Exception {
        submitKafkaToStarRocksJob(format);

        send(
                1,
                value(
                        newFields(),
                        "{\"id\":2147483648,\"name\":\"new\",\"email\":\"new@example.com\"}"));
        validateSinkSchema(
                Arrays.asList(
                        "id | bigint | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "email | varchar(1048576) | YES | false | null"));
        validateSinkResult(3, Collections.singletonList("2147483648 | new | new@example.com"));

        send(0, value(oldFields(), "{\"id\":2,\"name\":\"old\"}"));
        validateSinkResult(
                3, Arrays.asList("2 | old | null", "2147483648 | new | new@example.com"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testReplayIntToStringAlterFromHistoricalOffset(EventFormat format) throws Exception {
        submitKafkaToStarRocksJob(format);

        LOG.info("Historical INT records...");
        send(0, value(intAgeFields(), "{\"id\":1,\"name\":\"alice\",\"age\":18}"));
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "age | int | YES | false | null"));
        validateSinkResult(3, Collections.singletonList("1 | alice | 18"));

        LOG.info("ALTER INT to STRING on the same partition...");
        send(0, value(stringAgeFields(), "{\"id\":2,\"name\":\"bob\",\"age\":\"hello\"}"));
        waitUntilStarRocksSchemaChangeIdle();
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "age | varchar(1048576) | YES | false | null"));
        validateSinkResult(3, Arrays.asList("1 | alice | 18", "2 | bob | hello"));

        LOG.info("Replay remaining historical INT records from another partition...");
        send(1, value(intAgeFields(), "{\"id\":3,\"name\":\"carol\",\"age\":19}"));
        validateSinkResult(3, Arrays.asList("1 | alice | 18", "2 | bob | hello", "3 | carol | 19"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testSamePartitionRenameKeepsOldColumnAndAddsNew(EventFormat format) throws Exception {
        submitKafkaToStarRocksJob(format);

        send(0, value(oldFields(), "{\"id\":1,\"name\":\"alice\"}"));
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null"));
        validateSinkResult(2, Collections.singletonList("1 | alice"));

        LOG.info("Source column name is replaced by full_name on the same partition...");
        send(0, value(renamedFields(), "{\"id\":2,\"full_name\":\"bob\"}"));
        waitUntilStarRocksSchemaChangeIdle();
        validateSinkSchema(
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(1048576) | YES | false | null",
                        "full_name | varchar(1048576) | YES | false | null"));
        validateSinkResult(3, Arrays.asList("1 | alice | null", "2 | null | bob"));

        LOG.info("Historical records that still use name arrive from another partition...");
        send(1, value(oldFields(), "{\"id\":3,\"name\":\"carol\"}"));
        validateSinkResult(
                3, Arrays.asList("1 | alice | null", "2 | null | bob", "3 | carol | null"));
    }

    private void submitKafkaToStarRocksJob(EventFormat format) throws Exception {
        eventFormat = format;
        Path kafkaJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path starRocksJar = TestUtils.getResource("starrocks-cdc-pipeline-connector.jar");
        submitPipelineJob(buildPipelineJob(), kafkaJar, starRocksJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
    }

    private String buildPipelineJob() {
        return String.format(
                "source:\n"
                        + "  type: kafka\n"
                        + "  topic: %s\n"
                        + "  group-id: %s\n"
                        + "  scan.startup.mode: earliest-offset\n"
                        + "  value.format: %s\n"
                        + "  properties.bootstrap.servers: %s:9092\n"
                        + "\n"
                        + "transform:\n"
                        + "  - source-table: %s.\\.*\n"
                        + "    primary-keys: id\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: starrocks\n"
                        + "  jdbc-url: jdbc:mysql://%s:9030\n"
                        + "  load-url: %s:8080\n"
                        + "  username: root\n"
                        + "  password: \"\"\n"
                        + "  table.create.properties.replication_num: 1\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: 2\n"
                        + "  schema.change.behavior: lenient\n",
                topic,
                UUID.randomUUID(),
                eventFormat.optionValue,
                KAFKA_ALIAS,
                DATABASE,
                STARROCKS_ALIAS,
                STARROCKS_ALIAS);
    }

    private void send(int partition, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(topic, partition, null, value)).get();
        producer.flush();
    }

    private void validateSinkResult(int columnCount, List<String> expected) throws Exception {
        waitAndVerify("SELECT * FROM " + qualifiedTable(), columnCount, expected, true);
    }

    private void validateSinkSchema(List<String> expected) throws Exception {
        waitAndVerify("DESCRIBE " + qualifiedTable(), 5, expected, false);
    }

    private void waitAndVerify(
            String sql, int numberOfColumns, List<String> expected, boolean inAnyOrder)
            throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        List<String> actual = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                actual = fetchTableContent(sql, numberOfColumns);
                if (inAnyOrder) {
                    if (expected.stream()
                            .sorted()
                            .collect(Collectors.toList())
                            .equals(actual.stream().sorted().collect(Collectors.toList()))) {
                        return;
                    }
                } else if (expected.equals(actual)) {
                    return;
                }
                LOG.info(
                        "Executing {} didn't get expected results.\nExpected: {}\n  Actual: {}\n  Alter: {}",
                        sql,
                        expected,
                        actual,
                        latestAlterState());
            } catch (SQLException t) {
                LOG.info(
                        "Table {} isn't ready yet. Waiting for the next loop...", qualifiedTable());
            }
            Thread.sleep(1000L);
        }
        Assertions.fail(
                String.format(
                        "Failed to verify content of %s::%s. Actual: %s", DATABASE, sql, actual));
    }

    private List<String> fetchTableContent(String sql, int columnCount) throws Exception {
        List<String> results = new ArrayList<>();
        try (Connection conn = STARROCKS_CONTAINER.createConnection("");
                Statement stat = conn.createStatement();
                ResultSet rs = stat.executeQuery(sql)) {
            while (rs.next()) {
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    try {
                        columns.add(rs.getString(i));
                    } catch (SQLException ignored) {
                        columns.add(null);
                    }
                }
                results.add(String.join(" | ", columns));
            }
        }
        return results;
    }

    private void waitUntilStarRocksSchemaChangeIdle() throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        long idleSince = -1L;
        String lastState = "ABSENT";
        while (System.currentTimeMillis() < deadline) {
            lastState = latestAlterState();
            if (lastState.startsWith("CANCELLED")) {
                Assertions.fail("StarRocks schema change was cancelled: " + lastState);
            }
            boolean running =
                    lastState.startsWith("PENDING")
                            || lastState.startsWith("WAITING_TXN")
                            || lastState.startsWith("RUNNING");
            if (running) {
                idleSince = -1L;
            } else if (idleSince < 0) {
                idleSince = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - idleSince >= 3000L) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assertions.fail(
                "Timed out waiting for StarRocks schema change to become idle, last state: "
                        + lastState);
    }

    private String latestAlterState() {
        try (Connection connection = STARROCKS_CONTAINER.createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute("USE `" + DATABASE + "`");
            try (ResultSet resultSet =
                    statement.executeQuery(
                            "SHOW ALTER TABLE COLUMN WHERE TableName = '"
                                    + table
                                    + "' ORDER BY CreateTime DESC LIMIT 1")) {
                if (!resultSet.next()) {
                    return "ABSENT";
                }
                String msg = resultSet.getString("Msg");
                return resultSet.getString("State") + (msg == null ? "" : "/" + msg);
            }
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private void dropTableQuietly() {
        try (Connection connection = STARROCKS_CONTAINER.createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + qualifiedTable());
        } catch (Exception e) {
            LOG.info("Failed to drop StarRocks table {}.", qualifiedTable(), e);
        }
    }

    private String qualifiedTable() {
        return "`" + DATABASE + "`.`" + table + "`";
    }

    private static void waitForStarRocksBackend() throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = STARROCKS_CONTAINER.createConnection("");
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SHOW BACKENDS")) {
                if (resultSet.next() && resultSet.getBoolean("Alive")) {
                    return;
                }
            } catch (Exception e) {
                LOG.info("StarRocks backend is not ready yet.", e);
            }
            Thread.sleep(1000L);
        }
        throw new RuntimeException("StarRocks backend startup timed out.");
    }

    private Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        return properties;
    }

    private byte[] value(String fields, String row) {
        if (eventFormat == EventFormat.DEBEZIUM_JSON) {
            return debeziumValue(fields, row);
        }
        if (eventFormat == EventFormat.CANAL_JSON) {
            return canalValue(fields, row);
        }
        throw new IllegalArgumentException("Unsupported event format " + eventFormat);
    }

    private byte[] debeziumValue(String fields, String row) {
        String rowSchema =
                "{\"type\":\"struct\",\"fields\":["
                        + fields
                        + "],\"optional\":true,\"name\":\""
                        + DATABASE
                        + "."
                        + table
                        + ".Value\"}";
        return bytes(
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + withField(rowSchema, "before")
                        + ","
                        + withField(rowSchema, "after")
                        + "]},\"payload\":{\"before\":null,\"after\":"
                        + row
                        + ",\"source\":{\"db\":\""
                        + DATABASE
                        + "\",\"table\":\""
                        + table
                        + "\"},\"op\":\"c\"}}");
    }

    private byte[] canalValue(String fields, String row) {
        return bytes(
                "{\"data\":["
                        + row
                        + "],\"database\":\""
                        + DATABASE
                        + "\",\"isDdl\":false,\"mysqlType\":{"
                        + fields
                        + "},\"old\":null,\"pkNames\":[\"id\"],\"table\":\""
                        + table
                        + "\",\"ts\":1589373560798,\"type\":\"INSERT\"}");
    }

    private String createFields() {
        return intField("id", false) + "," + stringField("name") + "," + intField("age", true);
    }

    private String intAgeFields() {
        return createFields();
    }

    private String stringAgeFields() {
        return intField("id", false) + "," + stringField("name") + "," + stringField("age");
    }

    private String addColumnFields() {
        return createFields() + "," + stringField("email");
    }

    private String modifyColumnFields() {
        return intField("id", false)
                + ","
                + stringField("name")
                + ","
                + longField("age", true)
                + ","
                + stringField("email");
    }

    private String oldFields() {
        return intField("id", false) + "," + stringField("name");
    }

    private String renamedFields() {
        return intField("id", false) + "," + stringField("full_name");
    }

    private String newFields() {
        return longField("id", false) + "," + stringField("name") + "," + stringField("email");
    }

    private String intField(String name, boolean optional) {
        return field("int32", "INTEGER", name, optional);
    }

    private String longField(String name, boolean optional) {
        return field("int64", "BIGINT", name, optional);
    }

    private String stringField(String name) {
        return field("string", "VARCHAR(255)", name, true);
    }

    private String field(String debeziumType, String canalType, String name, boolean optional) {
        if (eventFormat == EventFormat.DEBEZIUM_JSON) {
            return "{\"type\":\""
                    + debeziumType
                    + "\",\"optional\":"
                    + optional
                    + ",\"field\":\""
                    + name
                    + "\"}";
        }
        if (eventFormat == EventFormat.CANAL_JSON) {
            return "\"" + name + "\":\"" + canalType + "\"";
        }
        throw new IllegalArgumentException("Unsupported event format " + eventFormat);
    }

    private static String withField(String schema, String field) {
        return schema.substring(0, schema.length() - 1) + ",\"field\":\"" + field + "\"}";
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private enum EventFormat {
        DEBEZIUM_JSON("debezium-json"),
        CANAL_JSON("canal-json");

        private final String optionValue;

        EventFormat(String optionValue) {
            this.optionValue = optionValue;
        }
    }
}
