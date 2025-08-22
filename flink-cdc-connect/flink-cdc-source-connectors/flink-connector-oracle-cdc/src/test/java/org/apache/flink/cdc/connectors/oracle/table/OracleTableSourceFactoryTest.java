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

package org.apache.flink.cdc.connectors.oracle.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Test for {@link OracleTableSource} created by {@link OracleTableSourceFactory}. */
class OracleTableSourceFactoryTest {
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
                                    "database_name", DataTypes.STRING(), "database_name", true),
                            Column.metadata("table_name", DataTypes.STRING(), "table_name", true),
                            Column.metadata(
                                    "schema_name", DataTypes.STRING(), "schema_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final int MY_PORT = 1521;
    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "MYDB";
    private static final String MY_TABLE = "myTable";
    private static final String MY_SCHEMA = "mySchema";
    private static final Properties PROPERTIES = new Properties();

    @Test
    void testRequiredProperties() {
        Map<String, String> properties = getAllRequiredOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PROPERTIES,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        JdbcSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        JdbcSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllRequiredOptionsWithHost();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PROPERTIES,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllRequiredOptions();
        options.put("port", "1521");
        options.put("hostname", MY_LOCALHOST);
        options.put("debezium.snapshot.mode", "initial");
        options.put(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "initial");
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        dbzProperties,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        true,
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testScanIncrementalProperties() {
        Map<String, String> options = getAllRequiredOptions();
        int chunkSize = 4096;
        int splitMetaGroupSize = 2048;
        int fetchSize = 1024;
        Duration connectTimeout = Duration.ofSeconds(60);
        int connectMaxRetry = 5;
        int connectPoolSize = 10;
        double distributionFactorUpper = 40.5;
        double distributionFactorLower = 0.01;
        options.put("port", "1521");
        options.put("hostname", MY_LOCALHOST);
        options.put("debezium.snapshot.mode", "initial");
        options.put(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key(), "true");
        options.put(
                SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(),
                String.valueOf(chunkSize));
        options.put(SourceOptions.CHUNK_META_GROUP_SIZE.key(), String.valueOf(splitMetaGroupSize));
        options.put(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.key(), String.valueOf(fetchSize));
        options.put(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.key(), "true");
        options.put(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.key(), "true");
        options.put(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        options.put(
                SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.key(),
                "true");

        options.put(
                JdbcSourceOptions.CONNECT_TIMEOUT.key(),
                String.format("%ds", connectTimeout.getSeconds()));
        options.put(JdbcSourceOptions.CONNECT_MAX_RETRIES.key(), String.valueOf(connectMaxRetry));
        options.put(JdbcSourceOptions.CONNECTION_POOL_SIZE.key(), String.valueOf(connectPoolSize));
        options.put(
                JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                String.valueOf(distributionFactorUpper));
        options.put(
                JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                String.valueOf(distributionFactorLower));

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "initial");
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        dbzProperties,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        chunkSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetry,
                        connectPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        null,
                        true,
                        true,
                        true,
                        true);
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromInitial() {
        Map<String, String> properties = getAllRequiredOptionsWithHost();
        properties.put("scan.startup.mode", "initial");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PROPERTIES,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllRequiredOptionsWithHost();
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PROPERTIES,
                        StartupOptions.latest(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testMetadataColumns() {
        Map<String, String> properties = getAllRequiredOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        OracleTableSource oracleTableSource = (OracleTableSource) actualSource;
        oracleTableSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = oracleTableSource.copy();
        OracleTableSource expectedSource =
                new OracleTableSource(
                        SCHEMA_WITH_METADATA,
                        null,
                        MY_PORT,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        new Properties(),
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name");

        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testValidation() {
        // validate illegal port
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllRequiredOptionsWithHost();
                            properties.put("port", "123b");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Could not parse value '123b' for key 'port'.");

        // validate missing required
        Factory factory = new OracleTableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllRequiredOptionsWithHost();
            properties.remove(requiredOption.key());

            Assertions.assertThatThrownBy(() -> createTableSource(properties))
                    .hasStackTraceContaining(
                            "Missing required options are:\n\n" + requiredOption.key());
        }

        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllRequiredOptionsWithHost();
                            properties.put("unknown", "abc");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Unsupported options:\n\nunknown");

        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllRequiredOptionsWithHost();
                            properties.put("scan.startup.mode", "abc");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining(
                        "Invalid value for option 'scan.startup.mode'. Supported values are "
                                + "[initial, snapshot, latest-offset], "
                                + "but was: abc");
    }

    private Map<String, String> getAllRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "oracle-cdc");
        options.put("port", "1521");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("schema-name", MY_SCHEMA);
        return options;
    }

    private Map<String, String> getAllRequiredOptionsWithHost() {
        Map<String, String> options = getAllRequiredOptions();
        options.put("hostname", MY_LOCALHOST);
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
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
                OracleTableSourceFactoryTest.class.getClassLoader(),
                false);
    }
}
