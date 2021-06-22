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

package com.alibaba.ververica.cdc.connectors.oracle.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link com.alibaba.ververica.cdc.connectors.oracle.table.OracleTableSource} created by
 * {@link com.alibaba.ververica.cdc.connectors.oracle.table.OracleTableSourceFactory}.
 */
public class OracleTableSourceFactoryTest {
    private static final TableSchema SCHEMA =
            TableSchema.builder()
                    .field("aaa", DataTypes.INT().notNull())
                    .field("bbb", DataTypes.STRING().notNull())
                    .field("ccc", DataTypes.DOUBLE())
                    .field("ddd", DataTypes.DECIMAL(31, 18))
                    .field("eee", DataTypes.TIMESTAMP(3))
                    .primaryKey("bbb", "aaa")
                    .build();

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String MY_SCHEMA = "mySchema";
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        TableSchemaUtils.getPhysicalSchema(SCHEMA),
                        1521,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "1521");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("debezium.snapshot.mode", "initial");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "initial");
        OracleTableSource expectedSource =
                new OracleTableSource(
                        TableSchemaUtils.getPhysicalSchema(SCHEMA),
                        1521,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromInitial() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "initial");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        TableSchemaUtils.getPhysicalSchema(SCHEMA),
                        1521,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.startup.mode", "latest-offset");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        OracleTableSource expectedSource =
                new OracleTableSource(
                        TableSchemaUtils.getPhysicalSchema(SCHEMA),
                        1521,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_SCHEMA,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        PROPERTIES,
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

        // validate missing required
        Factory factory = new OracleTableSourceFactory();
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
                            + "[initial, latest-offset], "
                            + "but was: abc";

            assertTrue(ExceptionUtils.findThrowableWithMessage(t, msg).isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "oracle-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("schema-name", MY_SCHEMA);
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock source"),
                new Configuration(),
                OracleTableSourceFactoryTest.class.getClassLoader(),
                false);
    }
}
