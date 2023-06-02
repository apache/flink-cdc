/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.table;

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
import org.apache.flink.table.factories.FactoryUtil;

import com.ververica.cdc.connectors.base.options.JdbcSourceOptions;
import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.debezium.utils.ResolvedSchemaUtils;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Test for {@link SqlServerTableSource} created by {@link SqlServerTableFactory}. */
public class SqlServerTableFactoryTest {

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
                            Column.metadata("schema_name", DataTypes.STRING(), "schema_name", true),
                            Column.metadata("table_name", DataTypes.STRING(), "table_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "dbo.myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        SqlServerTableSource expectedSource =
                new SqlServerTableSource(
                        SCHEMA,
                        1433,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        ZoneId.of("UTC"),
                        MY_USERNAME,
                        MY_PASSWORD,
                        PROPERTIES,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "1433");
        options.put("debezium.snapshot.mode", "initial");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "initial");
        SqlServerTableSource expectedSource =
                new SqlServerTableSource(
                        SCHEMA,
                        1433,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        ZoneId.of("UTC"),
                        MY_USERNAME,
                        MY_PASSWORD,
                        dbzProperties,
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .defaultValue(),
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .defaultValue(),
                        null);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        SqlServerTableSource sqlServerTableSource = (SqlServerTableSource) actualSource;
        sqlServerTableSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name", "schema_name", "table_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = sqlServerTableSource.copy();
        SqlServerTableSource expectedSource =
                new SqlServerTableSource(
                        ResolvedSchemaUtils.getPhysicalSchema(SCHEMA_WITH_METADATA),
                        1433,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        ZoneId.of("UTC"),
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
                        null);
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "database_name", "schema_name", "table_name");

        assertEquals(expectedSource, actualSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "sqlserver-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
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
                SqlServerTableFactoryTest.class.getClassLoader(),
                false);
    }
}
