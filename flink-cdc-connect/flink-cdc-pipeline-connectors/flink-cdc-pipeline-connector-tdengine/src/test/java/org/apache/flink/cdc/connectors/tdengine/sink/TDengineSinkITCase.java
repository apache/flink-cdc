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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.tdengine.sink.utils.TDengineContainer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;

/** Integration tests for TDengine sink against a real TDengine instance. */
@Testcontainers(disabledWithoutDocker = true)
class TDengineSinkITCase {

    private static final TableId TABLE_ID = TableId.parse("inventory.metrics");
    private static final Schema SCHEMA = TDengineTestUtils.SCHEMA;

    @Container private static final TDengineContainer TDENGINE = new TDengineContainer();

    private TDengineDataSinkConfig sinkConfig;

    @BeforeEach
    void setUp() throws SQLException {
        sinkConfig = createSinkConfig(true);
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS test_db");
        }
    }

    @Test
    void testCreateStableWriteAndQuery() throws Exception {
        TDengineMetadataApplier metadataApplier = new TDengineMetadataApplier(sinkConfig);
        metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));

        try (TDengineEventWriter writer = new TDengineEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            TABLE_ID, TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D)),
                    null);
            writer.flush(false);
        }

        Assertions.assertThat(queryDouble("SELECT temperature FROM test_db.device_1 LIMIT 1"))
                .isEqualTo(12.5D);
        Assertions.assertThat(queryString("SELECT location FROM test_db.device_1 LIMIT 1"))
                .isEqualTo("room-a");
    }

    @Test
    void testSchemaValidationRejectsDrift() throws Exception {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS test_db");
            statement.execute(
                    "CREATE STABLE IF NOT EXISTS test_db.metrics "
                            + "(ts TIMESTAMP, temperature FLOAT, status NCHAR(32)) "
                            + "TAGS (location NCHAR(32))");
        }

        TDengineMetadataApplier metadataApplier = new TDengineMetadataApplier(sinkConfig);
        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, SCHEMA)))
                .hasMessageContaining("schema mismatch");
    }

    @Test
    void testAddColumnAndWriteNewField() throws Exception {
        TDengineMetadataApplier metadataApplier = new TDengineMetadataApplier(sinkConfig);
        metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));
        metadataApplier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("humidity", DataTypes.DOUBLE())))));

        Schema evolvedSchema =
                Schema.newBuilder()
                        .physicalColumn("ts", DataTypes.BIGINT().notNull())
                        .physicalColumn("device_id", DataTypes.STRING().notNull())
                        .physicalColumn("location", DataTypes.STRING())
                        .physicalColumn("temperature", DataTypes.DOUBLE())
                        .physicalColumn("status", DataTypes.STRING())
                        .physicalColumn("humidity", DataTypes.DOUBLE())
                        .build();

        try (TDengineEventWriter writer = new TDengineEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(TABLE_ID, evolvedSchema), null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            TABLE_ID,
                            GenericRecordDataWithHumidity(
                                    1000L, "device-1", "room-a", 12.5D, 55.0D)),
                    null);
            writer.flush(false);
        }

        Assertions.assertThat(queryDouble("SELECT humidity FROM test_db.device_1 LIMIT 1"))
                .isEqualTo(55.0D);
    }

    private static org.apache.flink.cdc.common.data.RecordData GenericRecordDataWithHumidity(
            long ts, String deviceId, String location, double temperature, double humidity) {
        return org.apache.flink.cdc.common.data.GenericRecordData.of(
                ts,
                org.apache.flink.cdc.common.data.binary.BinaryStringData.fromString(deviceId),
                org.apache.flink.cdc.common.data.binary.BinaryStringData.fromString(location),
                temperature,
                org.apache.flink.cdc.common.data.binary.BinaryStringData.fromString("ok"),
                humidity);
    }

    private TDengineDataSinkConfig createSinkConfig(boolean stableSchemaValidationEnabled) {
        return new TDengineDataSinkConfig(
                TDENGINE.getJdbcUrl(),
                TDengineContainer.DEFAULT_USERNAME,
                TDengineContainer.DEFAULT_PASSWORD,
                "test_db",
                "metrics",
                Collections.emptyMap(),
                true,
                "ts",
                "device_id",
                Collections.singletonList("location"),
                true,
                true,
                stableSchemaValidationEnabled,
                256,
                "nchar",
                "double",
                ZoneId.of("UTC"),
                100,
                900_000,
                Duration.ofSeconds(10),
                3,
                Duration.ofMillis(200),
                "reject",
                "upsert",
                "reject",
                "reject",
                Collections.emptyMap());
    }

    private Connection openConnection() throws SQLException {
        registerDriver(TDENGINE.getJdbcUrl());
        Properties properties = new Properties();
        properties.setProperty("user", TDengineContainer.DEFAULT_USERNAME);
        properties.setProperty("password", TDengineContainer.DEFAULT_PASSWORD);
        return DriverManager.getConnection(TDENGINE.getJdbcUrl(), properties);
    }

    private double queryDouble(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertThat(resultSet.next()).isTrue();
            return resultSet.getDouble(1);
        }
    }

    private String queryString(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertThat(resultSet.next()).isTrue();
            return resultSet.getString(1);
        }
    }

    private static void registerDriver(String jdbcUrl) throws SQLException {
        try {
            DriverManager.getDriver(jdbcUrl);
            return;
        } catch (SQLException ignored) {
            // Register explicitly below.
        }
        try {
            Class<?> clazz =
                    Class.forName(
                            "com.taosdata.jdbc.ws.WebSocketDriver",
                            true,
                            Thread.currentThread().getContextClassLoader());
            Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(driver);
        } catch (ReflectiveOperationException e) {
            throw new SQLException("Failed to register TDengine JDBC driver.", e);
        }
    }
}
