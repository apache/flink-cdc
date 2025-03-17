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

package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
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

import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.JDBC_DRIVER;
import static org.junit.Assert.assertEquals;

/** Unit tests for TiDB table source factory. */
public class TiDBTableSourceFactoryTest {

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
                            Column.metadata("op_ts", DataTypes.TIMESTAMP(), "op_ts", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_HOSTNAME = "tidb0";
    private static final String MY_DATABASE = "inventory";
    private static final String MY_PORT = "4000";
    private static final String MY_TABLE = "products";
    private static final String MY_USERNAME = "root";
    private static final String MY_PASSWORD = "";
    private static final String PD_ADDRESS = "pd0:2379";
    private static final String HOST_MAPPING = "host1:1;host2:2;host3:3";
    private static final Map<String, String> OPTIONS = new HashMap<>();
    private static final Properties PROPERTIES = new Properties();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        System.out.println(actualSource.asSummaryString());
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        Integer.parseInt(MY_PORT),
                        MY_HOSTNAME,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC").toString(),
                        PROPERTIES,
                        false,
                        HEARTBEAT_INTERVAL.defaultValue(),
                        PD_ADDRESS,
                        HOST_MAPPING,
                        CONNECT_TIMEOUT.defaultValue(),
                        OPTIONS,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        null,
                        new HashMap<>(),
                        JDBC_DRIVER.defaultValue(),
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("port", MY_PORT);
        properties.put("scan.startup.mode", "initial");
        properties.put("heartbeat.interval.ms", "15213ms");
        //        properties.put("server-time-zone", "Asia/Shanghai");

        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        dbzProperties.put("offset.flush.interval.ms", "3000");
        dbzProperties.put("tombstones.on.delete", "true");
        dbzProperties.put("test", "test");

        DynamicTableSource actualSource = createTableSource(properties);
        Map<String, String> options = new HashMap<>();
        //        options.put("tikv.grpc.timeout_in_ms", "20000");
        //        options.put("tikv.grpc.scan_timeout_in_ms", "20000");
        //        options.put("tikv.batch_get_concurrency", "4");
        //        options.put("tikv.batch_put_concurrency", "4");
        //        options.put("tikv.batch_scan_concurrency", "4");
        //        options.put("tikv.batch_delete_concurrency", "4");
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        4111,
                        MY_HOSTNAME,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("UTC").toString(),
                        dbzProperties,
                        false,
                        Duration.ofMillis(15213),
                        PD_ADDRESS,
                        HOST_MAPPING,
                        CONNECT_TIMEOUT.defaultValue(),
                        OPTIONS,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        null,
                        null,
                        JDBC_DRIVER.defaultValue(),
                        StartupOptions.initial());
        assertEquals(expectedSource, actualSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "tidb-cdc");
        options.put("hostname", MY_HOSTNAME);
        options.put("port", MY_PORT);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("pd-addresses", PD_ADDRESS);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("host-mapping", HOST_MAPPING);
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
                TiDBTableSourceFactoryTest.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }
}
