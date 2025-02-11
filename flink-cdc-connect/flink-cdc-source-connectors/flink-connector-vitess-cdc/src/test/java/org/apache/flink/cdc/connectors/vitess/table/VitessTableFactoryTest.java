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

package org.apache.flink.cdc.connectors.vitess.table;

import org.apache.flink.cdc.connectors.vitess.config.SchemaAdjustmentMode;
import org.apache.flink.cdc.connectors.vitess.config.TabletType;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.api.TableSchema.fromResolvedSchema;

/** Test for {@link VitessTableSource} created by {@link VitessTableFactory}. */
class VitessTableFactoryTest {

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
    private static final String MY_KEYSPACE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final Properties PROPERTIES = new Properties();

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        VitessTableSource expectedSource =
                new VitessTableSource(
                        SCHEMA,
                        15991,
                        MY_LOCALHOST,
                        MY_KEYSPACE,
                        MY_TABLE,
                        null,
                        null,
                        null,
                        "current",
                        false,
                        true,
                        SchemaAdjustmentMode.AVRO,
                        TabletType.RDONLY,
                        "decoderbufs",
                        "flink",
                        PROPERTIES);
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "5444");
        options.put("decoding.plugin.name", "wal2json");
        options.put("debezium.snapshot.mode", "never");
        options.put("name", "flink");
        options.put("tablet-type", "MASTER");
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        VitessTableSource expectedSource =
                new VitessTableSource(
                        SCHEMA,
                        5444,
                        MY_LOCALHOST,
                        MY_KEYSPACE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        null,
                        "current",
                        false,
                        true,
                        SchemaAdjustmentMode.AVRO,
                        TabletType.MASTER,
                        "wal2json",
                        "flink",
                        dbzProperties);
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
        Factory factory = new VitessTableFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());

            Assertions.assertThatThrownBy(
                            () -> {
                                createTableSource(properties);
                            })
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

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "vitess-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("keyspace", MY_KEYSPACE);
        options.put("table-name", MY_TABLE);
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
                VitessTableFactoryTest.class.getClassLoader(),
                false);
    }
}
