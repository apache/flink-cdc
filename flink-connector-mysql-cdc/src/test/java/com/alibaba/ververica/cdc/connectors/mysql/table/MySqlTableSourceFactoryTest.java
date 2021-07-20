/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.table;

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
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_FETCH_SIZE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_OPTIMIZE_INTEGRAL_KEY;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SPLIT_SIZE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SNAPSHOT_PARALLEL_SCAN;
import static org.apache.flink.table.api.TableSchema.fromResolvedSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link MySqlTableSource} created by {@link MySqlTableSourceFactory}. */
public class MySqlTableSourceFactoryTest {

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

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        null,
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testEnableParallelReadSource() {
        Map<String, String> properties = getAllOptions();
        properties.put("snapshot.parallel-scan", "true");
        properties.put("server-id", "123,126");
        properties.put("scan.split.column", "aaa");
        properties.put("scan.split.size", "8000");
        properties.put("scan.fetch.size", "100");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        "123,126",
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        true,
                        8000,
                        100,
                        "aaa",
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testDisableIntegralOptimization() {
        Map<String, String> properties = getAllOptions();
        properties.put("snapshot.parallel-scan", "true");
        properties.put("server-id", "123,126");
        properties.put("scan.split.column", "aaa");
        properties.put("scan.split.size", "8000");
        properties.put("scan.fetch.size", "100");
        properties.put("scan.optimize.integral-key", "false");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        "123,126",
                        false,
                        true,
                        8000,
                        100,
                        "aaa",
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "3307");
        options.put("server-id", "4321");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("debezium.snapshot.mode", "never");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3307,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
                        "4321",
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromSpecificOffset() {
        final String offsetFile = "mysql-bin.000003";
        final int offsetPos = 100203;

        Map<String, String> options = getAllOptions();
        options.put("port", "3307");
        options.put("server-id", "4321");
        options.put("scan.startup.mode", "specific-offset");
        options.put("scan.startup.specific-offset.file", offsetFile);
        options.put("scan.startup.specific-offset.pos", String.valueOf(offsetPos));

        DynamicTableSource actualSource = createTableSource(options);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3307,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        "4321",
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.specificOffset(offsetFile, offsetPos));
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromInitial() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "initial");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        null,
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromEarliestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "earliest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        null,
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.earliest());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        MySqlTableSource expectedSource =
                new MySqlTableSource(
                        TableSchemaUtils.getPhysicalSchema(fromResolvedSchema(SCHEMA)),
                        3306,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        null,
                        SCAN_OPTIMIZE_INTEGRAL_KEY.defaultValue(),
                        SNAPSHOT_PARALLEL_SCAN.defaultValue(),
                        SCAN_SPLIT_SIZE.defaultValue(),
                        SCAN_FETCH_SIZE.defaultValue(),
                        null,
                        StartupOptions.latest());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        // validate illegal port
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("port", "123b");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t, "Could not parse value '123b' for key 'port'.")
                            .isPresent());
        }

        // validate illegal server id
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("server-id", "123b");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The 'server.id' should contains single numeric ID, but is 123b")
                            .isPresent());
        }

        // validate illegal server id range
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("snapshot.parallel-scan", "true");
            properties.put("server-id", "123");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The server id should be a range syntax like '5400,5404' when enable 'snapshot.parallel-scan' to 'true', but actual is 123")
                            .isPresent());
        }

        // validate split key when use combined primary key
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("snapshot.parallel-scan", "true");
            properties.put("server-id", "123,126");
            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The 'scan.split.column' option is required if the primary key contains multiple fields")
                            .isPresent());
        }

        // validate split key must belong to primary key
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("snapshot.parallel-scan", "true");
            properties.put("server-id", "123,126");
            properties.put("scan.split.column", "ddd");
            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The 'scan.split.column' value ddd should be one field of the primary key [bbb, aaa], but it does not.")
                            .isPresent());
        }

        // validate missing required
        Factory factory = new MySqlTableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
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
            Map<String, String> properties = getAllOptions();
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
            Map<String, String> properties = getAllOptions();
            properties.put("scan.startup.mode", "abc");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            String msg =
                    "Invalid value for option 'scan.startup.mode'. Supported values are "
                            + "[initial, earliest-offset, latest-offset, specific-offset, timestamp], "
                            + "but was: abc";
            assertTrue(ExceptionUtils.findThrowableWithMessage(t, msg).isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mysql-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                fromResolvedSchema(SCHEMA).toSchema(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        SCHEMA),
                new Configuration(),
                MySqlTableSourceFactoryTest.class.getClassLoader(),
                false);
    }
}
