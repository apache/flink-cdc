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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
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
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY;
import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertProducedTypeOfSourceFunction;

/** Test for {@link PostgreSQLTableSource} created by {@link PostgreSQLTableFactory}. */
class PostgreSQLTableFactoryTest {

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

    private static final ResolvedSchema SCHEMA_WITHOUT_PRIMARY_KEY =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    new ArrayList<>(),
                    null);

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.BIGINT().notNull()),
                            Column.physical("name", DataTypes.STRING()),
                            Column.physical("count", DataTypes.DECIMAL(38, 18)),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata("row_kind", DataTypes.STRING(), "row_kind", true),
                            Column.metadata(
                                    "database_name", DataTypes.STRING(), "database_name", true),
                            Column.metadata("schema_name", DataTypes.STRING(), "schema_name", true),
                            Column.metadata("table_name", DataTypes.STRING(), "table_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String MY_SCHEMA = "public";
    private static final String MY_SLOT_NAME = "flinktest";
    private static final Properties PROPERTIES = new Properties();
    private static final boolean SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT =
            SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue();

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        PostgreSQLTableSource expectedSource =
                new PostgreSQLTableSource(
                        SCHEMA,
                        5432,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        "decoderbufs",
                        MY_SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED.defaultValue(),
                        SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "5444");
        options.put("decoding.plugin.name", "wal2json");
        options.put("debezium.snapshot.mode", "never");
        options.put("changelog-mode", "upsert");
        options.put("scan.incremental.snapshot.backfill.skip", "true");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("scan.read-changelog-as-append-only.enabled", "true");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        PostgreSQLTableSource expectedSource =
                new PostgreSQLTableSource(
                        SCHEMA,
                        5444,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        "wal2json",
                        MY_SLOT_NAME,
                        DebeziumChangelogMode.UPSERT,
                        dbzProperties,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        true,
                        true,
                        SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        true,
                        SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        PostgreSQLTableSource postgreSQLTableSource = (PostgreSQLTableSource) actualSource;
        postgreSQLTableSource.applyReadableMetadata(
                Arrays.asList("row_kind", "op_ts", "database_name", "schema_name", "table_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = postgreSQLTableSource.copy();
        PostgreSQLTableSource expectedSource =
                new PostgreSQLTableSource(
                        ResolvedSchemaUtils.getPhysicalSchema(SCHEMA_WITH_METADATA),
                        5432,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        "decoderbufs",
                        MY_SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        new Properties(),
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED.defaultValue(),
                        SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.defaultValue());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("row_kind", "op_ts", "database_name", "schema_name", "table_name");

        Assertions.assertThat(actualSource).isEqualTo(expectedSource);

        ScanTableSource.ScanRuntimeProvider provider =
                postgreSQLTableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        DebeziumSourceFunction<RowData> debeziumSourceFunction =
                (DebeziumSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();
        assertProducedTypeOfSourceFunction(debeziumSourceFunction, expectedSource.producedDataType);
    }

    @Test
    void testEnableParallelReadSource() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("scan.incremental.snapshot.chunk.size", "8000");
        properties.put("scan.snapshot.fetch.size", "100");
        properties.put("connect.timeout", "45s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        PostgreSQLTableSource expectedSource =
                new PostgreSQLTableSource(
                        SCHEMA,
                        5432,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        "decoderbufs",
                        MY_SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        true,
                        8000,
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        100,
                        Duration.ofSeconds(45),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED.defaultValue(),
                        SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("scan.incremental.snapshot.chunk.size", "8000");
        properties.put("scan.snapshot.fetch.size", "100");
        properties.put("connect.timeout", "45s");
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        PostgreSQLTableSource expectedSource =
                new PostgreSQLTableSource(
                        SCHEMA,
                        5432,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        "decoderbufs",
                        MY_SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        true,
                        8000,
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        100,
                        Duration.ofSeconds(45),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        HEARTBEAT_INTERVAL.defaultValue(),
                        StartupOptions.latest(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED.defaultValue(),
                        SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.defaultValue());
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

        // validate missing required
        Factory factory = new PostgreSQLTableFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());

            Assertions.assertThatThrownBy(() -> createTableSource(SCHEMA, properties))
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
    }

    @Test
    void testUpsertModeWithoutPrimaryKeyError() {
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("changelog-mode", "upsert");

                            createTableSource(SCHEMA_WITHOUT_PRIMARY_KEY, properties);
                        })
                .hasStackTraceContaining(
                        "Primary key must be present when upsert mode is selected.");
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "postgres-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("schema-name", MY_SCHEMA);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("slot.name", MY_SLOT_NAME);
        options.put("scan.incremental.snapshot.enabled", String.valueOf(false));
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
                PostgreSQLTableFactoryTest.class.getClassLoader(),
                false);
    }
}
