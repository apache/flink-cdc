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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.DockerImageVersions.KAFKA;

/**
 * End-to-end tests for multi-partition Kafka Debezium and Canal JSON to Paimon pipelines, covering
 * create table, add column, alter column type, DML, cross-partition historical replay,
 * rename-as-superset and multi-table sync.
 */
class KafkaToPaimonE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToPaimonE2eITCase.class);

    private static final Duration PAIMON_TESTCASE_TIMEOUT = Duration.ofMinutes(3);
    private static final String DATABASE = "inventory";
    private static final String KAFKA_ALIAS = "kafka";

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(KAFKA_ALIAS);

    private AdminClient admin;
    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private String table;
    private String warehouse;
    private EventFormat eventFormat;

    @BeforeAll
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        topic = "kafka-customers-" + UUID.randomUUID();
        table = "customers_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        warehouse = sharedVolume.toString() + "/paimon_" + UUID.randomUUID();
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(
                        TestUtils.getResource(getPaimonSQLConnectorResourceName())),
                sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName());
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-shade-hadoop.jar")),
                sharedVolume.toString() + "/flink-shade-hadoop.jar");
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
        super.after();
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testCreateTableAddColumnModifyColumnAndDml(EventFormat format) throws Exception {
        submitKafkaToPaimonJob(format);

        LOG.info("Create table and snapshot/insert records...");
        send(0, value(createFields(), "c", "null", "{\"id\":1,\"name\":\"alice\",\"age\":18}"));
        send(0, value(createFields(), "r", "null", "{\"id\":10,\"name\":\"snapshot\",\"age\":30}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING", "age, INT"));
        validateSinkResult(Arrays.asList("1, alice, 18", "10, snapshot, 30"));

        LOG.info("Add column...");
        send(
                0,
                value(
                        addColumnFields(),
                        "c",
                        "null",
                        "{\"id\":2,\"name\":\"bob\",\"age\":21,\"email\":\"bob@example.com\"}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING", "age, INT", "email, STRING"));
        validateSinkResult(
                Arrays.asList(
                        "1, alice, 18, null",
                        "10, snapshot, 30, null",
                        "2, bob, 21, bob@example.com"));

        LOG.info("Alter column type INT to BIGINT...");
        send(
                0,
                value(
                        modifyColumnFields(),
                        "c",
                        "null",
                        "{\"id\":3,\"name\":\"charlie\",\"age\":40,\"email\":\"charlie@example.com\"}"));
        validateSinkSchema(
                Arrays.asList("id, INT", "name, STRING", "age, BIGINT", "email, STRING"));
        validateSinkResult(
                Arrays.asList(
                        "1, alice, 18, null",
                        "10, snapshot, 30, null",
                        "2, bob, 21, bob@example.com",
                        "3, charlie, 40, charlie@example.com"));

        LOG.info("Update and delete by primary key...");
        send(
                0,
                value(
                        modifyColumnFields(),
                        "u",
                        "{\"id\":1,\"name\":\"alice\",\"age\":18}",
                        "{\"id\":1,\"name\":\"alice2\",\"age\":18}"));
        send(
                0,
                value(
                        modifyColumnFields(),
                        "d",
                        "{\"id\":2,\"name\":\"bob\",\"age\":21,\"email\":\"bob@example.com\"}",
                        "null"));
        validateSinkResult(
                Arrays.asList(
                        "1, alice2, 18, null",
                        "10, snapshot, 30, null",
                        "3, charlie, 40, charlie@example.com"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testNewSchemaThenHistoricalSchemaFromAnotherPartition(EventFormat format)
            throws Exception {
        submitKafkaToPaimonJob(format);

        send(
                1,
                value(
                        newFields(),
                        "c",
                        "null",
                        "{\"id\":2147483648,\"name\":\"new\",\"email\":\"new@example.com\"}"));
        validateSinkSchema(Arrays.asList("id, BIGINT", "name, STRING", "email, STRING"));
        validateSinkResult(Collections.singletonList("2147483648, new, new@example.com"));

        send(0, value(oldFields(), "c", "null", "{\"id\":2,\"name\":\"old\"}"));
        validateSinkResult(Arrays.asList("2, old, null", "2147483648, new, new@example.com"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testReplayIntToStringAlterFromHistoricalOffset(EventFormat format) throws Exception {
        submitKafkaToPaimonJob(format);

        LOG.info("Historical INT records...");
        send(0, value(intAgeFields(), "c", "null", "{\"id\":1,\"name\":\"alice\",\"age\":18}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING", "age, INT"));
        validateSinkResult(Collections.singletonList("1, alice, 18"));

        LOG.info("ALTER INT to STRING on the same partition...");
        send(
                0,
                value(
                        stringAgeFields(),
                        "c",
                        "null",
                        "{\"id\":2,\"name\":\"bob\",\"age\":\"hello\"}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING", "age, STRING"));
        validateSinkResult(Arrays.asList("1, alice, 18", "2, bob, hello"));

        LOG.info("Replay remaining historical INT records from another partition...");
        send(1, value(intAgeFields(), "c", "null", "{\"id\":3,\"name\":\"carol\",\"age\":19}"));
        validateSinkResult(Arrays.asList("1, alice, 18", "2, bob, hello", "3, carol, 19"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testSamePartitionRenameKeepsOldColumnAndAddsNew(EventFormat format) throws Exception {
        submitKafkaToPaimonJob(format);

        send(0, value(oldFields(), "c", "null", "{\"id\":1,\"name\":\"alice\"}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING"));
        validateSinkResult(Collections.singletonList("1, alice"));

        LOG.info("Source column name is replaced by full_name on the same partition...");
        send(0, value(renamedFields(), "c", "null", "{\"id\":2,\"full_name\":\"bob\"}"));
        validateSinkSchema(Arrays.asList("id, INT", "name, STRING", "full_name, STRING"));
        validateSinkResult(Arrays.asList("1, alice, null", "2, null, bob"));

        LOG.info("Historical records that still use name arrive from another partition...");
        send(1, value(oldFields(), "c", "null", "{\"id\":3,\"name\":\"carol\"}"));
        validateSinkResult(Arrays.asList("1, alice, null", "2, null, bob", "3, carol, null"));
    }

    @ParameterizedTest(name = "format: {0}")
    @EnumSource(EventFormat.class)
    void testMultiTableFromSameTopic(EventFormat format) throws Exception {
        String orders = "orders_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        submitKafkaToPaimonJob(format);

        send(0, value(table, oldFields(), "c", "null", "{\"id\":1,\"name\":\"alice\"}"));
        send(1, value(orders, orderFields(), "c", "null", "{\"id\":1001,\"amount\":19}"));
        validateSinkSchema(table, Arrays.asList("id, INT", "name, STRING"));
        validateSinkSchema(orders, Arrays.asList("id, INT", "amount, INT"));
        validateSinkResult(table, Collections.singletonList("1, alice"));
        validateSinkResult(orders, Collections.singletonList("1001, 19"));

        LOG.info("Add column independently on both tables...");
        send(
                0,
                value(
                        table,
                        oldFields() + "," + stringField("city"),
                        "c",
                        "null",
                        "{\"id\":2,\"name\":\"bob\",\"city\":\"berlin\"}"));
        send(
                1,
                value(
                        orders,
                        orderFields() + "," + stringField("status"),
                        "c",
                        "null",
                        "{\"id\":1002,\"amount\":7,\"status\":\"paid\"}"));
        validateSinkSchema(table, Arrays.asList("id, INT", "name, STRING", "city, STRING"));
        validateSinkSchema(orders, Arrays.asList("id, INT", "amount, INT", "status, STRING"));
        validateSinkResult(table, Arrays.asList("1, alice, null", "2, bob, berlin"));
        validateSinkResult(orders, Arrays.asList("1001, 19, null", "1002, 7, paid"));
    }

    private void submitKafkaToPaimonJob(EventFormat format) throws Exception {
        eventFormat = format;
        Path kafkaJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path paimonJar = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(buildPipelineJob(), kafkaJar, paimonJar, hadoopJar);
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
                        + "  type: paimon\n"
                        + "  catalog.properties.warehouse: %s\n"
                        + "  catalog.properties.metastore: filesystem\n"
                        + "  catalog.properties.cache-enabled: false\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: 2\n"
                        + "  schema.change.behavior: lenient\n",
                topic,
                UUID.randomUUID(),
                eventFormat.optionValue,
                KAFKA_ALIAS,
                DATABASE,
                warehouse);
    }

    private void send(int partition, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(topic, partition, null, value)).get();
        producer.flush();
    }

    private void validateSinkResult(List<String> expected) throws InterruptedException {
        validateSinkResult(table, expected);
    }

    private void validateSinkResult(String tableName, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} results...", warehouse, DATABASE, tableName);
        long deadline = System.currentTimeMillis() + PAIMON_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchPaimonRows("docker/peek-paimon.sql", tableName);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + PAIMON_TESTCASE_TIMEOUT.toMillis())
                                / 1000);
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private void validateSinkSchema(List<String> expected) throws InterruptedException {
        validateSinkSchema(table, expected);
    }

    private void validateSinkSchema(String tableName, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} schema...", warehouse, DATABASE, tableName);
        long deadline = System.currentTimeMillis() + PAIMON_TESTCASE_TIMEOUT.toMillis();
        List<String> actual = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                actual =
                        fetchPaimonRows("docker/peek-paimon-schema.sql", tableName).stream()
                                .map(KafkaToPaimonE2eITCase::normalizeSchemaRow)
                                .collect(Collectors.toList());
                Assertions.assertThat(actual).containsExactlyElementsOf(expected);
                return;
            } catch (Exception e) {
                LOG.warn("Schema validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                LOG.warn("Schema mismatch.\nExpected: {}\n  Actual: {}", expected, actual);
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(actual).containsExactlyElementsOf(expected);
    }

    private List<String> fetchPaimonRows(String sqlResource, String tableName) throws Exception {
        String template =
                readLines(sqlResource).stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, DATABASE, tableName);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to execute peek script. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(KafkaToPaimonE2eITCase::extractRow)
                .map(row -> String.join(", ", row))
                .collect(Collectors.toList());
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private static String normalizeSchemaRow(String row) {
        String[] parts = row.split(", ");
        Assertions.assertThat(parts.length)
                .as("Unexpected DESCRIBE row: %s", row)
                .isGreaterThanOrEqualTo(2);
        return stripIdentifier(parts[0]) + ", " + normalizeType(parts[1]);
    }

    private static String stripIdentifier(String value) {
        return value.replace("`", "");
    }

    private static String normalizeType(String type) {
        String normalized = stripIdentifier(type).replace(" NOT NULL", "").trim();
        if (normalized.equalsIgnoreCase("INTEGER")) {
            return "INT";
        }
        if (normalized.equalsIgnoreCase("STRING")
                || normalized.equalsIgnoreCase("VARCHAR(2147483647)")) {
            return "STRING";
        }
        return normalized;
    }

    private Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        return properties;
    }

    private byte[] value(String fields, String operation, String before, String after) {
        return value(table, fields, operation, before, after);
    }

    private byte[] value(
            String tableName, String fields, String operation, String before, String after) {
        if (eventFormat == EventFormat.DEBEZIUM_JSON) {
            return debeziumValue(tableName, fields, operation, before, after);
        }
        if (eventFormat == EventFormat.CANAL_JSON) {
            return canalValue(tableName, fields, operation, before, after);
        }
        throw new IllegalArgumentException("Unsupported event format " + eventFormat);
    }

    private byte[] debeziumValue(
            String tableName, String fields, String operation, String before, String after) {
        String rowSchema =
                "{\"type\":\"struct\",\"fields\":["
                        + fields
                        + "],\"optional\":true,\"name\":\""
                        + DATABASE
                        + "."
                        + tableName
                        + ".Value\"}";
        return bytes(
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + withField(rowSchema, "before")
                        + ","
                        + withField(rowSchema, "after")
                        + "]},\"payload\":{\"before\":"
                        + before
                        + ",\"after\":"
                        + after
                        + ",\"source\":{\"db\":\""
                        + DATABASE
                        + "\",\"table\":\""
                        + tableName
                        + "\"},\"op\":\""
                        + operation
                        + "\"}}");
    }

    private byte[] canalValue(
            String tableName, String fields, String operation, String before, String after) {
        String data = "d".equals(operation) ? before : after;
        String old = "u".equals(operation) ? asArray(before) : "null";
        return bytes(
                "{\"data\":"
                        + asArray(data)
                        + ",\"database\":\""
                        + DATABASE
                        + "\",\"isDdl\":false,\"mysqlType\":{"
                        + fields
                        + "},\"old\":"
                        + old
                        + ",\"pkNames\":[\"id\"],\"table\":\""
                        + tableName
                        + "\",\"ts\":1589373560798,\"type\":\""
                        + canalOperation(operation)
                        + "\"}");
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

    private String orderFields() {
        return intField("id", false) + "," + intField("amount", true);
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

    private static String asArray(String row) {
        return "null".equals(row) ? "null" : "[" + row + "]";
    }

    private static String canalOperation(String operation) {
        switch (operation) {
            case "c":
            case "r":
                return "INSERT";
            case "u":
                return "UPDATE";
            case "d":
                return "DELETE";
            default:
                throw new IllegalArgumentException("Unsupported operation " + operation);
        }
    }

    private String getPaimonSQLConnectorResourceName() {
        return String.format("paimon-sql-connector-%s.jar", flinkVersion);
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
