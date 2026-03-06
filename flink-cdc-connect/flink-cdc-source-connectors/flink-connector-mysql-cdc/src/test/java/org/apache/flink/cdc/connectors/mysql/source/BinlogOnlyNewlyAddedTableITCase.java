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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowUtils;

import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * IT tests for binlog-only newly added table capture functionality using {@link
 * MySqlSourceBuilder#scanBinlogNewlyAddedTableEnabled(boolean)}.
 *
 * <p>This test validates that tables matching the configured pattern are automatically captured
 * when they are created during binlog reading phase, without triggering snapshot phase.
 */
class BinlogOnlyNewlyAddedTableITCase extends MySqlSourceTestBase {

    private final UniqueDatabase testDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "binlog_test", "mysqluser", "mysqlpw");

    @BeforeEach
    public void before() throws SQLException {
        testDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        testDatabase.dropDatabase();
    }

    @Test
    void testBinlogOnlyCaptureSingleNewTable() throws Exception {
        testBinlogOnlyCapture("products_2024");
    }

    @Test
    void testBinlogOnlyCaptureMultipleNewTables() throws Exception {
        testBinlogOnlyCapture("orders_2024", "orders_2025");
    }

    @Test
    void testBinlogOnlyCaptureWithPatternMatching() throws Exception {
        // Test with wildcard pattern: capture tables like user_*
        // Flink CDC style: unescaped '.' is db/table separator, '\.' is regex any-char wildcard
        testBinlogOnlyCaptureWithPattern(
                testDatabase.getDatabaseName() + ".user_\\.*",
                "user_profiles",
                "user_settings",
                "user_logs");
    }

    @Test
    void testBinlogOnlyCaptureWithDatabasePattern() throws Exception {
        // Test with database.* pattern (all tables in database)
        // Flink CDC style: unescaped '.' is db/table separator, '\.' is regex any-char wildcard
        testBinlogOnlyCaptureWithPattern(
                testDatabase.getDatabaseName() + ".\\.*", "product_inventory", "product_catalog");
    }

    private void testBinlogOnlyCapture(String... tableNames) throws Exception {
        String pattern =
                testDatabase.getDatabaseName() + "\\.(" + String.join("|", tableNames) + ")";
        testBinlogOnlyCaptureWithPattern(pattern, tableNames);
    }

    private void testBinlogOnlyCaptureWithPattern(String tablePattern, String... tableNames)
            throws Exception {
        // Pre-create tables before starting source to satisfy startup validation.
        // With StartupOptions.latest(), no snapshot is taken - only binlog events after source
        // starts are captured.
        try (MySqlConnection preConnection = getConnection()) {
            preConnection.setAutoCommit(false);
            for (String tableName : tableNames) {
                String tableId = testDatabase.getDatabaseName() + "." + tableName;
                preConnection.execute(
                        format(
                                "CREATE TABLE %s (id BIGINT PRIMARY KEY, name VARCHAR(100), quantity INT);",
                                tableId));
            }
            preConnection.commit();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        RowDataDebeziumDeserializeSchema deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setMetadataConverters(
                                new MetadataConverter[] {
                                    MySqlReadableMetadata.TABLE_NAME.getConverter()
                                })
                        .setPhysicalRowType(
                                (RowType)
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "quantity", DataTypes.INT()))
                                                .getLogicalType())
                        .setResultTypeInfo(
                                InternalTypeInfo.of(
                                        TypeConversions.fromDataToLogicalType(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "quantity", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "_table_name",
                                                                DataTypes.STRING().notNull())))))
                        .build();

        // Build source with binlog-only auto-capture enabled
        MySqlSource<RowData> mySqlSource =
                MySqlSource.<RowData>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(tablePattern)
                        .username(testDatabase.getUsername())
                        .password(testDatabase.getPassword())
                        .serverTimeZone("UTC")
                        .serverId("6001-6004")
                        .startupOptions(StartupOptions.latest())
                        .deserializer(deserializer)
                        .scanBinlogNewlyAddedTableEnabled(true)
                        .build();

        DataStreamSource<RowData> source =
                env.fromSource(
                        mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Binlog CDC Source");

        CollectResultIterator<RowData> iterator = addCollectSink(source);
        JobClient jobClient = env.executeAsync("Binlog-Only Newly Added Table Test");
        iterator.setJobClient(jobClient);

        // Wait for job to start reading binlog
        Thread.sleep(2000);

        // Insert/update/delete data - these are captured as binlog events
        List<String> expectedResults = new ArrayList<>();
        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);

            for (String tableName : tableNames) {
                String tableId = testDatabase.getDatabaseName() + "." + tableName;

                // Insert data - these should be captured as binlog events
                connection.execute(
                        format(
                                "INSERT INTO %s VALUES (1, '%s_item1', 10), (2, '%s_item2', 20);",
                                tableId, tableName, tableName));

                // Update data
                connection.execute(format("UPDATE %s SET quantity = 15 WHERE id = 1;", tableId));

                // Delete data
                connection.execute(format("DELETE FROM %s WHERE id = 2;", tableId));

                connection.commit();

                // Expected results: INSERT + UPDATE + DELETE events (no snapshot data)
                expectedResults.addAll(
                        Arrays.asList(
                                format("+I[1, %s_item1, 10, %s]", tableName, tableName),
                                format("+I[2, %s_item2, 20, %s]", tableName, tableName),
                                format("-U[1, %s_item1, 10, %s]", tableName, tableName),
                                format("+U[1, %s_item1, 15, %s]", tableName, tableName),
                                format("-D[2, %s_item2, 20, %s]", tableName, tableName)));
            }
        }

        // Wait for events to be processed
        Thread.sleep(2000);

        // Verify captured events
        List<String> actualResults = fetchRowData(iterator, expectedResults.size());
        assertEqualsInAnyOrder(expectedResults, actualResults);

        jobClient.cancel().get();
    }

    @Test
    void testBinlogOnlyDoesNotCaptureNonMatchingTables() throws Exception {
        // Pre-create matching table before starting source (required for startup validation)
        String matchingTable = testDatabase.getDatabaseName() + ".temp_test";
        try (MySqlConnection preConnection = getConnection()) {
            preConnection.setAutoCommit(false);
            preConnection.execute(
                    format(
                            "CREATE TABLE %s (id BIGINT PRIMARY KEY, value VARCHAR(100));",
                            matchingTable));
            preConnection.commit();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        RowDataDebeziumDeserializeSchema deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setMetadataConverters(
                                new MetadataConverter[] {
                                    MySqlReadableMetadata.TABLE_NAME.getConverter()
                                })
                        .setPhysicalRowType(
                                (RowType)
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                        DataTypes.FIELD(
                                                                "value", DataTypes.STRING()))
                                                .getLogicalType())
                        .setResultTypeInfo(
                                InternalTypeInfo.of(
                                        TypeConversions.fromDataToLogicalType(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                        DataTypes.FIELD(
                                                                "value", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "_table_name",
                                                                DataTypes.STRING().notNull())))))
                        .build();

        // Only capture tables matching temp_*
        // Flink CDC style: unescaped '.' is db/table separator, '\.' is regex any-char wildcard
        String tablePattern = testDatabase.getDatabaseName() + ".temp_\\.*";

        MySqlSource<RowData> mySqlSource =
                MySqlSource.<RowData>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(tablePattern)
                        .username(testDatabase.getUsername())
                        .password(testDatabase.getPassword())
                        .serverTimeZone("UTC")
                        .serverId("6005-6008")
                        .startupOptions(StartupOptions.latest())
                        .deserializer(deserializer)
                        .scanBinlogNewlyAddedTableEnabled(true)
                        .build();

        DataStreamSource<RowData> source =
                env.fromSource(
                        mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Binlog CDC Source");

        CollectResultIterator<RowData> iterator = addCollectSink(source);
        JobClient jobClient = env.executeAsync("Binlog-Only Non-Matching Table Test");
        iterator.setJobClient(jobClient);

        Thread.sleep(2000);

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);

            // Insert into matching table (already exists)
            connection.execute(format("INSERT INTO %s VALUES (1, 'matched');", matchingTable));

            // Create and insert into non-matching table (will not be captured)
            String nonMatchingTable = testDatabase.getDatabaseName() + ".permanent_test";
            connection.execute(
                    format(
                            "CREATE TABLE %s (id BIGINT PRIMARY KEY, value VARCHAR(100));",
                            nonMatchingTable));
            connection.execute(
                    format("INSERT INTO %s VALUES (2, 'not_matched');", nonMatchingTable));

            connection.commit();
        }

        Thread.sleep(2000);

        // Should only capture the matching table's events
        List<String> expectedResults = Arrays.asList("+I[1, matched, temp_test]");
        List<String> actualResults = fetchRowData(iterator, expectedResults.size());

        assertEqualsInAnyOrder(expectedResults, actualResults);

        jobClient.cancel().get();
    }

    private CollectResultIterator<RowData> addCollectSink(DataStreamSource<RowData> stream) {
        TypeSerializer<RowData> serializer =
                stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<RowData> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<RowData> operator =
                (CollectSinkOperator<RowData>) factory.getOperator();
        CollectStreamSink<RowData> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Binlog Collect Sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        return new CollectResultIterator(
                operator.getOperatorIdFuture(),
                serializer,
                accumulatorName,
                stream.getExecutionEnvironment().getCheckpointConfig(),
                10000L);
    }

    private List<String> fetchRowData(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        long deadline = System.currentTimeMillis() + 10000; // 10s timeout
        while (size > 0 && System.currentTimeMillis() < deadline) {
            if (iter.hasNext()) {
                RowData row = iter.next();
                rows.add(row);
                size--;
            } else {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        return convertRowDataToRowString(rows);
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        if (rows.isEmpty()) {
            return new ArrayList<>();
        }

        // Determine the schema based on the first row
        int fieldCount = rows.get(0).getArity();

        if (fieldCount == 4) {
            // Schema: id, name, quantity, _table_name
            LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
            map.put("id", 0);
            map.put("name", 1);
            map.put("quantity", 2);
            map.put("_table_name", 3);
            return rows.stream()
                    .map(
                            row ->
                                    RowUtils.createRowWithNamedPositions(
                                                    row.getRowKind(),
                                                    new Object[] {
                                                        row.getLong(0),
                                                        row.getString(1).toString(),
                                                        row.getInt(2),
                                                        row.getString(3).toString()
                                                    },
                                                    map)
                                            .toString())
                    .collect(Collectors.toList());
        } else {
            // Schema: id, value, _table_name
            LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
            map.put("id", 0);
            map.put("value", 1);
            map.put("_table_name", 2);
            return rows.stream()
                    .map(
                            row ->
                                    RowUtils.createRowWithNamedPositions(
                                                    row.getRowKind(),
                                                    new Object[] {
                                                        row.getLong(0),
                                                        row.getString(1).toString(),
                                                        row.getString(2).toString()
                                                    },
                                                    map)
                                            .toString())
                    .collect(Collectors.toList());
        }
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", testDatabase.getUsername());
        properties.put("database.password", testDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }
}
