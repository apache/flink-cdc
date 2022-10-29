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

package com.ververica.cdc.connectors.oracle.table;

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
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link com.ververica.cdc.connectors.oracle.table.OracleTableSource} created by {@link
 * com.ververica.cdc.connectors.oracle.table.OracleTableSourceFactory}.
 */
public class OracleTableSourceFactoryTest {
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
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String MY_SCHEMA = "mySchema";
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testRequiredProperties() {
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
                        true);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testCommonProperties() {
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
                        true);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllRequiredOptions();
        options.put("port", "1521");
        options.put("hostname", MY_LOCALHOST);
        options.put("debezium.snapshot.mode", "initial");

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
                        true);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromInitial() {
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
                        true);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromLatestOffset() {
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
                        true);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testMetadataColumns() {
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
                        true);
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name");

        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        // validate illegal port
        try {
            Map<String, String> properties = getAllRequiredOptionsWithHost();
            properties.put("port", "123b");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t, "Could not parse value '123b' for key 'port'.")
                            .isPresent());
        }

        // validate missing required
        Factory factory = new OracleTableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllRequiredOptionsWithHost();
            properties.remove(requiredOption.key());

            try {
                createTableSource(properties);
                fail("exception expected");
            } catch (Throwable t) {
                assertTrue(
                        ExceptionUtils.findThrowableWithMessage(
                                        t,
                                        "Missing required options are:\n\n" + requiredOption.key())
                                .isPresent());
            }
        }

        // validate unsupported option
        try {
            Map<String, String> properties = getAllRequiredOptionsWithHost();
            properties.put("unknown", "abc");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }

        // validate unsupported option
        try {
            Map<String, String> properties = getAllRequiredOptionsWithHost();
            properties.put("scan.startup.mode", "abc");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            String msg =
                    "Invalid value for option 'scan.startup.mode'. Supported values are "
                            + "[initial, latest-offset], "
                            + "but was: abc";

            assertTrue(ExceptionUtils.findThrowableWithMessage(t, msg).isPresent());
        }
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
