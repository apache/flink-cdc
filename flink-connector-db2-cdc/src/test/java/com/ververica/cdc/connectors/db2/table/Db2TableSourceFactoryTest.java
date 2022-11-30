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

package com.ververica.cdc.connectors.db2.table;

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
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ververica.cdc.connectors.utils.AssertUtils.assertProducedTypeOfSourceFunction;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.table.api.TableSchema.fromResolvedSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link Db2TableSource} created by {@link Db2TableSourceFactory}. */
public class Db2TableSourceFactoryTest {

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
    private static final String MY_SCHEMA = "flinkuser";
    private static final String MY_TABLE = "myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties, SCHEMA);
        Db2TableSource expectedSource =
                new Db2TableSource(
                        getPhysicalSchema(SCHEMA),
                        50000,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
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
        options.put("port", "50000");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("debezium.snapshot.mode", "schema_only");

        DynamicTableSource actualSource = createTableSource(options, SCHEMA);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "schema_only");
        Db2TableSource expectedSource =
                new Db2TableSource(
                        getPhysicalSchema(SCHEMA),
                        50000,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
                        StartupOptions.latest());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        // validate illegal port
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("port", "123b");

            createTableSource(properties, SCHEMA);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t, "Could not parse value '123b' for key 'port'.")
                            .isPresent());
        }

        // validate missing required
        Factory factory = new Db2TableSourceFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());

            try {
                createTableSource(properties, SCHEMA);
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

            createTableSource(properties, SCHEMA);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }
    }

    @Test
    public void testMetadataColumns() {
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
                        MY_SCHEMA,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC"),
                        new Properties(),
                        StartupOptions.initial());
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "database_name", "table_name", "schema_name");

        assertEquals(expectedSource, actualSource);

        ScanTableSource.ScanRuntimeProvider provider =
                db2TableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        DebeziumSourceFunction<RowData> debeziumSourceFunction =
                (DebeziumSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();
        assertProducedTypeOfSourceFunction(debeziumSourceFunction, expectedSource.producedDataType);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "db2-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("schema-name", MY_SCHEMA);
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
