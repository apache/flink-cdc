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

package org.apache.flink.cdc.connectors.db2.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.table.api.TableSchema.fromResolvedSchema;

/** Test for {@link Db2TableSource} created by {@link Db2TableSourceFactory}. */
class Db2TableSourceFactoryTest {

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
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3)),
                            Column.metadata(
                                    "database_name", DataTypes.STRING(), "database_name", true),
                            Column.metadata("table_name", DataTypes.STRING(), "table_name", true),
                            Column.metadata("schema_name", DataTypes.STRING(), "schema_name", true),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("bbb", "aaa")));

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "flinkuser.myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties, SCHEMA);
        Db2TableSource expectedSource =
                new Db2TableSource(
                        getPhysicalSchema(SCHEMA),
                        50000,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
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
                        false,
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "50000");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("debezium.snapshot.mode", "schema_only");
        options.put("scan.incremental.snapshot.unbounded-chunk-first.enabled", "true");

        DynamicTableSource actualSource = createTableSource(options, SCHEMA);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "schema_only");
        Db2TableSource expectedSource =
                new Db2TableSource(
                        getPhysicalSchema(SCHEMA),
                        50000,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
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
                        false,
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        true);
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testValidation() {
        // validate illegal port
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("port", "123b");
                            createTableSource(properties, SCHEMA);
                        })
                .hasStackTraceContaining("Could not parse value '123b' for key 'port'.");

        // validate missing required
        Factory factory = new Db2TableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());

            Assertions.assertThatThrownBy(() -> createTableSource(properties, SCHEMA))
                    .hasStackTraceContaining(
                            "Missing required options are:\n\n" + requiredOption.key());
        }

        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("unknown", "abc");

                            createTableSource(properties, SCHEMA);
                        })
                .hasStackTraceContaining("Unsupported options:\n\nunknown");
    }

    @Test
    void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties, SCHEMA_WITH_METADATA);
        Db2TableSource db2TableSource = (Db2TableSource) actualSource;
        db2TableSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = db2TableSource.copy();
        Db2TableSource expectedSource =
                new Db2TableSource(
                        SCHEMA_WITH_METADATA,
                        50000,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
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
                        false,
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                                .defaultValue());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name");

        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "db2-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        return options;
    }

    private static DynamicTableSource createTableSource(
            Map<String, String> options, ResolvedSchema schema) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                fromResolvedSchema(schema).toSchema(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        schema),
                new Configuration(),
                Db2TableSourceFactoryTest.class.getClassLoader(),
                false);
    }
}
