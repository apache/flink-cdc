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

package org.apache.flink.cdc.connectors.mysql;

import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Integration tests for the legacy {@link MySqlSource}. */
class LegacyMySqlSourceITCase extends LegacyMySqlTestBase {

    private final UniqueDatabase fullTypesDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", "mysqluser", "mysqlpw");

    @Test
    void testConsumingAllEventsWithJsonFormatIncludeSchema() throws Exception {
        testConsumingAllEventsWithJsonFormat(true);
    }

    @Test
    void testConsumingAllEventsWithJsonFormatExcludeSchema() throws Exception {
        testConsumingAllEventsWithJsonFormat(false);
    }

    @Test
    void testConsumingAllEventsWithJsonFormatWithNumericDecimal() throws Exception {
        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        testConsumingAllEventsWithJsonFormat(
                false,
                customConverterConfigs,
                "file/debezium-data-schema-exclude-with-numeric-decimal.json");
    }

    private void testConsumingAllEventsWithJsonFormat(
            Boolean includeSchema, Map<String, Object> customConverterConfigs, String expectedFile)
            throws Exception {
        fullTypesDatabase.createAndInitialize();
        JsonDebeziumDeserializationSchema schema =
                customConverterConfigs == null
                        ? new JsonDebeziumDeserializationSchema(includeSchema)
                        : new JsonDebeziumDeserializationSchema(
                                includeSchema, customConverterConfigs);
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        // monitor all tables under column_type_test database
                        .databaseList(fullTypesDatabase.getDatabaseName())
                        .username(fullTypesDatabase.getUsername())
                        .password(fullTypesDatabase.getPassword())
                        .deserializer(schema)
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        final JsonNode expected =
                new ObjectMapper().readValue(readLines(expectedFile), JsonNode.class);
        JsonNode expectSnapshot = expected.get("expected_snapshot");

        DataStreamSource<String> source = env.addSource(sourceFunction);
        tEnv.createTemporaryView("full_types", source);
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");

        // check the snapshot result
        CloseableIterator<Row> snapshot = result.collect();
        waitForSnapshotStarted(snapshot);

        assertJsonEquals(extractJsonBody(snapshot.next()), expectSnapshot);
        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        // check the binlog result
        CloseableIterator<Row> binlog = result.collect();
        JsonNode expectBinlog = expected.get("expected_binlog");
        assertJsonEquals(extractJsonBody(binlog.next()), expectBinlog);
        result.getJobClient().get().cancel().get();
    }

    private void testConsumingAllEventsWithJsonFormat(Boolean includeSchema) throws Exception {
        String expectedFile =
                includeSchema
                        ? "file/debezium-data-schema-include.json"
                        : "file/debezium-data-schema-exclude.json";
        testConsumingAllEventsWithJsonFormat(includeSchema, null, expectedFile);
    }

    private static List<Object> fetchRows(Iterator<Row> iter, int size) {
        List<Object> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            // ignore rowKind marker
            rows.add(row.getField(0));
            size--;
        }
        return rows;
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }

    private static byte[] readLines(String resource) throws IOException, URISyntaxException {
        Path path =
                Paths.get(
                        Objects.requireNonNull(
                                        LegacyMySqlSourceITCase.class
                                                .getClassLoader()
                                                .getResource(resource))
                                .toURI());
        return Files.readAllBytes(path);
    }

    private static void assertJsonEquals(JsonNode actual, JsonNode expect) throws Exception {
        if (actual.get("payload") != null && expect.get("payload") != null) {
            actual = actual.get("payload");
            expect = expect.get("payload");
        }
        Assertions.assertThat(actual.get("after")).isEqualTo(expect.get("after"));
        Assertions.assertThat(actual.get("before")).isEqualTo(expect.get("before"));
        Assertions.assertThat(actual.get("op")).isEqualTo(expect.get("op"));
    }

    private static JsonNode extractJsonBody(Row row) {
        try {
            String body = row.toString();
            return new ObjectMapper()
                    .readValue(body.substring(3, body.length() - 1), JsonNode.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Invalid JSON format.", e);
        }
    }
}
