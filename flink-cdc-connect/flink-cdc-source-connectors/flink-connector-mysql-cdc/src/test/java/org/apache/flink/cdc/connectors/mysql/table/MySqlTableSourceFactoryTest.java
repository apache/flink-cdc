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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.USE_LEGACY_JSON_FORMAT;

/** Test for {@link MySqlTableSource} created by {@link MySqlTableSourceFactory}. */
class MySqlTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("bbb", "aaa")));

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.BIGINT().notNull()),
                            Column.physical("name", DataTypes.STRING()),
                            Column.physical("count", DataTypes.DECIMAL(38, 18)),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata(
                                    "database_name", DataTypes.STRING(), "database_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSource() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("server-id", "123-126");
        properties.put("scan.incremental.snapshot.chunk.size", "8000");
        properties.put("chunk-meta.group.size", "3000");
        properties.put("chunk-key.even-distribution.factor.upper-bound", "40.5");
        properties.put("chunk-key.even-distribution.factor.lower-bound", "0.01");
        properties.put("scan.snapshot.fetch.size", "100");
        properties.put("connect.timeout", "45s");
        properties.put("scan.incremental.snapshot.chunk.key-column", "testCol");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        "123-126",
                        true,
                        8000,
                        3000,
                        100,
                        Duration.ofSeconds(45),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        40.5d,
                        0.01d,
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        "testCol",
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSourceWithSingleServerId() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("server-id", "123");
        properties.put("scan.incremental.snapshot.chunk.size", "8000");
        properties.put("scan.snapshot.fetch.size", "100");
        properties.put("connect.timeout", "45s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        "123",
                        true,
                        8000,
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        100,
                        Duration.ofSeconds(45),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSourceLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("server-id", "123-126");
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        "123-126",
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.latest(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "3307");
        options.put("server-id", "4321");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("debezium.snapshot.mode", "never");
        options.put("debezium.offset.flush.interval.ms", "3000");
        options.put("debezium.tombstones.on.delete", "true");
        options.put("debezium.test", "test");
        options.put("jdbc.properties.useSSL", "false");
        options.put("heartbeat.interval", "15213ms");
        options.put("scan.incremental.snapshot.chunk.key-column", "testCol");
        options.put("scan.incremental.close-idle-reader.enabled", "true");
        options.put("scan.incremental.snapshot.backfill.skip", "true");
        options.put("use.legacy.json.format", "true");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        dbzProperties.put("offset.flush.interval.ms", "3000");
        dbzProperties.put("tombstones.on.delete", "true");
        dbzProperties.put("test", "test");
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("useSSL", "false");
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3307,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
                        "4321",
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        true,
                        true,
                        jdbcProperties,
                        Duration.ofMillis(15213),
                        "testCol",
                        true,
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        true,
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource)
                .isEqualTo(expectedSource)
                .isInstanceOf(MySqlTableSource.class);
        MySqlTableSource actualMySqlTableSource = (MySqlTableSource) actualSource;
        Properties parellelProperties = new Properties();
        parellelProperties.put("test", "test");
        Assertions.assertThat(
                        actualMySqlTableSource.getParallelDbzProperties(
                                actualMySqlTableSource.getDbzProperties()))
                .isEqualTo(parellelProperties);
    }

    @Test
    void testStartupFromSpecificOffset() {
        final String offsetFile = "mysql-bin.000003";
        final int offsetPos = 100203;

        Map<String, String> properties = getAllOptions();
        properties.put("port", "3307");
        properties.put("server-id", "4321");
        properties.put("scan.startup.mode", "specific-offset");
        properties.put("scan.startup.specific-offset.file", offsetFile);
        properties.put("scan.startup.specific-offset.pos", String.valueOf(offsetPos));

        createTableSource(properties);
        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3307,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        "4321",
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.specificOffset(offsetFile, offsetPos),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromInitial() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "initial");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromEarliestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "earliest-offset");
        createTableSource(properties);
        createTableSource(properties);
        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.earliest(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromSpecificTimestamp() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "timestamp");
        properties.put("scan.startup.timestamp-millis", "0");
        createTableSource(properties);

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.timestamp(0L),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.latest(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        MySqlTableSource mySqlSource = (MySqlTableSource) actualSource;
        mySqlSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = mySqlSource.copy();

        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        ResolvedSchemaUtils.getPhysicalSchema(SCHEMA_WITH_METADATA),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        PARSE_ONLINE_SCHEMA_CHANGES.defaultValue(),
                        USE_LEGACY_JSON_FORMAT.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST.defaultValue());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys = Arrays.asList("op_ts", "database_name");

        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testValidation() {
        // validate illegal port
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("port", "123b");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Could not parse value '123b' for key 'port'.");

        // validate illegal server id
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("server-id", "123b");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("The value of option 'server-id' is invalid: '123b'")
                .hasStackTraceContaining("The server id 123b is not a valid numeric.");

        // validate illegal connect.timeout
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("connect.timeout", "240ms");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'connect.timeout' cannot be less than PT0.25S, but actual is PT0.24S");

        // validate illegal split size
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("scan.incremental.snapshot.chunk.size", "1");

                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'scan.incremental.snapshot.chunk.size' must larger than 1, but is 1");

        // validate illegal fetch size

        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("scan.snapshot.fetch.size", "1");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'scan.snapshot.fetch.size' must larger than 1, but is 1");

        // validate illegal split meta group size
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("chunk-meta.group.size", "1");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'chunk-meta.group.size' must larger than 1, but is 1");

        // validate illegal split meta group size
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("split-key.even-distribution.factor.upper-bound", "0.8");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'chunk-key.even-distribution.factor.upper-bound' must larger than or equals 1.0, but is 0.8");

        // validate illegal connection pool size
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("connection.pool.size", "1");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'connection.pool.size' must larger than 1, but is 1");

        // validate illegal connect max retry times
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("connect.max-retries", "0");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "The value of option 'connect.max-retries' must larger than 0, but is 0");

        // validate missing required
        Factory factory = new MySqlTableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());
            Assertions.assertThatThrownBy(() -> createTableSource(properties))
                    .hasStackTraceContaining(
                            "Missing required options are:\n\n" + requiredOption.key());
        }

        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("unknown", "abc");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Unsupported options:\n\nunknown");

        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.startup.mode", "abc");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "Invalid value for option 'scan.startup.mode'. Supported values are "
                                + "[initial, snapshot, latest-offset, earliest-offset, specific-offset, timestamp], "
                                + "but was: abc");

        // validate invalid database-name
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("database-name", "*_invalid_db");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        String.format(
                                "The database-name '%s' is not a valid regular expression",
                                "*_invalid_db"));

        // validate invalid table-name
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("table-name", "*_invalid_table");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        String.format(
                                "The table-name '%s' is not a valid regular expression",
                                "*_invalid_table"));
    }

    @Test
    void testEnablingExperimentalOptions() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.parse.online.schema.changes.enabled", "true");
        properties.put("use.legacy.json.format", "true");
        properties.put("scan.incremental.snapshot.unbounded-chunk-first.enabled", "true");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        SCHEMA,
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        PROPERTIES,
                        null,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        StartupOptions.initial(),
                        false,
                        false,
                        new Properties(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        null,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        true,
                        true,
                        true);
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mysql-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("scan.incremental.snapshot.enabled", String.valueOf(false));
        return options;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        schema),
                new Configuration(),
                MySqlTableSourceFactoryTest.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }
}
