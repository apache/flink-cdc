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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private Map<String, String> options;

    @BeforeEach
    void beforeEach() {
        options = getAllOptions();
    }

    @Test
    void testCommonProperties() {
        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSource() {
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("server-id", "123-126");
        options.put("scan.incremental.snapshot.chunk.size", "8000");
        options.put("chunk-meta.group.size", "3000");
        options.put("chunk-key.even-distribution.factor.upper-bound", "40.5");
        options.put("chunk-key.even-distribution.factor.lower-bound", "0.01");
        options.put("scan.snapshot.fetch.size", "100");
        options.put("connect.timeout", "45s");
        options.put("scan.incremental.snapshot.chunk.key-column", "testCol");

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSourceWithSingleServerId() {
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("server-id", "123");
        options.put("scan.incremental.snapshot.chunk.size", "8000");
        options.put("scan.snapshot.fetch.size", "100");
        options.put("connect.timeout", "45s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testEnableParallelReadSourceLatestOffset() {
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("server-id", "123-126");
        options.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        options.put("port", "3307");
        options.put("server-id", "4321");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("debezium.snapshot.mode", "never");
        options.put("jdbc.properties.useSSL", "false");
        options.put("heartbeat.interval", "15213ms");
        options.put("scan.incremental.snapshot.chunk.key-column", "testCol");
        options.put("scan.incremental.close-idle-reader.enabled", "true");
        options.put("scan.incremental.snapshot.backfill.skip", "true");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
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
                        true);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromSpecificOffset() {
        final String offsetFile = "mysql-bin.000003";
        final int offsetPos = 100203;

        options.put("port", "3307");
        options.put("server-id", "4321");
        options.put("scan.startup.mode", "specific-offset");
        options.put("scan.startup.specific-offset.file", offsetFile);
        options.put("scan.startup.specific-offset.pos", String.valueOf(offsetPos));

        createTableSource(options);
        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromInitial() {
        options.put("scan.startup.mode", "initial");

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromEarliestOffset() {
        options.put("scan.startup.mode", "earliest-offset");
        createTableSource(options);
        createTableSource(options);
        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromSpecificTimestamp() {
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp-millis", "0");
        createTableSource(options);

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromLatestOffset() {
        options.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testMetadataColumns() {
        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
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
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys = Arrays.asList("op_ts", "database_name");

        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testValidationIllegalPort() {
        // validate illegal port
        options.put("port", "123b");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining("Could not parse value '123b' for key 'port'.");
    }

    @Test
    void testValidationIllegalServerId() {
        // validate illegal server id
        options.put("server-id", "123b");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining("The value of option 'server-id' is invalid: '123b'")
                .hasStackTraceContaining("The server id 123b is not a valid numeric.");
    }

    @Test
    void testValidationIllegalSplitSize() {
        // validate illegal split size
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("scan.incremental.snapshot.chunk.size", "1");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'scan.incremental.snapshot.chunk.size' must larger than 1, but is 1");
    }

    @Test
    void testValidationIllegalFetchSize() {
        // validate illegal fetch size
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("scan.snapshot.fetch.size", "1");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'scan.snapshot.fetch.size' must larger than 1, but is 1");
    }

    @Test
    void testValidationIllegalChunkMetaGroupSize() {
        // validate illegal chunk meta group size
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("chunk-meta.group.size", "1");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'chunk-meta.group.size' must larger than 1, but is 1");
    }

    @ParameterizedTest
    @ValueSource(
            strings = { //
                "split-key.even-distribution.factor.upper-bound", //
                "chunk-key.even-distribution.factor.upper-bound" //
            })
    void testValidationIllegalChunkKeyUpperBound(final String optionKey) {
        // validate illegal chunk/split upper bound
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put(optionKey, "0.8");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'chunk-key.even-distribution.factor.upper-bound' must larger than or equals 1.0, but is 0.8");
    }

    @Test
    void testValidationIllegalConnectionPoolSize() {
        // validate illegal connection pool size
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("connection.pool.size", "1");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'connection.pool.size' must larger than 1, but is 1");
    }

    @Test
    void testValidationIllegalConnectMaxRetryTimes() {
        // validate illegal connect max retry times
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("connect.max-retries", "0");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(
                        "The value of option 'connect.max-retries' must larger than 0, but is 0");
    }

    @Test
    void testValidationMissingRequiredOptions() {
        // validate missing required
        Factory factory = new MySqlTableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());
            assertThatThrownBy(() -> createTableSource(properties))
                    .hasStackTraceContaining(
                            "Missing required options are:\n\n" + requiredOption.key());
        }
    }

    @Test
    void testValidationUnsupportedOption() {
        // validate unsupported option
        options.put("unknown", "abc");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining("Unsupported options:\n\nunknown");
    }

    @Test
    void testValidationUnsupportedOptionValue() {
        // validate unsupported option
        options.put("scan.startup.mode", "abc");
        final String expectedExceptionMessage =
                "Invalid value for option 'scan.startup.mode'. Supported values are "
                        + "[initial, snapshot, latest-offset, earliest-offset, specific-offset, timestamp], "
                        + "but was: abc";
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(expectedExceptionMessage);
    }

    @Test
    void testValidationInvalidDatabaseName() {
        // validate invalid database-name
        options.put("database-name", "*_invalid_db");
        final String expectedExceptionMessage =
                String.format(
                        "The database-name '%s' is not a valid regular expression", "*_invalid_db");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(expectedExceptionMessage);
    }

    @Test
    void testValidationInvalidTableName() {
        // validate invalid table-name
        options.put("table-name", "*_invalid_table");
        final String expectedExceptionMessage =
                String.format(
                        "The table-name '%s' is not a valid regular expression", "*_invalid_table");
        assertThatThrownBy(() -> createTableSource(options))
                .hasStackTraceContaining(expectedExceptionMessage);
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
