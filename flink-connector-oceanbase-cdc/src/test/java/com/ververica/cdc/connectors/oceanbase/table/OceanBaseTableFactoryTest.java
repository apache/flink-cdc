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

package com.ververica.cdc.connectors.oceanbase.table;

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
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link OceanBaseTableSource} created by {@link OceanBaseTableSourceFactory}. */
public class OceanBaseTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("aaa")));

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3)),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata("tenant", DataTypes.STRING(), "tenant_name", true),
                            Column.metadata("database", DataTypes.STRING(), "database_name", true),
                            Column.metadata("table", DataTypes.STRING(), "table_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("aaa")));

    private static final String STARTUP_MODE = "latest-offset";
    private static final String USERNAME = "user@sys";
    private static final String PASSWORD = "pswd";
    private static final String TENANT_NAME = "sys";
    private static final String DATABASE_NAME = "db[0-9]";
    private static final String TABLE_NAME = "table[0-9]";
    private static final String TABLE_LIST = "db.table";
    private static final String SERVER_TIME_ZONE = "+00:00";
    private static final String CONNECT_TIMEOUT = "30s";
    private static final String HOSTNAME = "127.0.0.1";
    private static final Integer PORT = 2881;
    private static final String LOG_PROXY_HOST = "127.0.0.1";
    private static final Integer LOG_PROXY_PORT = 2983;
    private static final String LOG_PROXY_CLIENT_ID = "clientId";
    private static final String RS_LIST = "127.0.0.1:2882:2881";
    private static final String WORKING_MODE = "storage";

    @Test
    public void testCommonProperties() {
        Map<String, String> options = getRequiredOptions();
        options.put("database-name", DATABASE_NAME);
        options.put("table-name", TABLE_NAME);
        options.put("table-list", TABLE_LIST);
        options.put("rootserver-list", RS_LIST);

        DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        OceanBaseTableSource expectedSource =
                new OceanBaseTableSource(
                        SCHEMA,
                        StartupMode.LATEST_OFFSET,
                        USERNAME,
                        PASSWORD,
                        TENANT_NAME,
                        DATABASE_NAME,
                        TABLE_NAME,
                        TABLE_LIST,
                        SERVER_TIME_ZONE,
                        Duration.parse("PT" + CONNECT_TIMEOUT),
                        null,
                        null,
                        LOG_PROXY_HOST,
                        LOG_PROXY_PORT,
                        null,
                        null,
                        RS_LIST,
                        null,
                        WORKING_MODE);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getRequiredOptions();
        options.put("scan.startup.mode", "initial");
        options.put("database-name", DATABASE_NAME);
        options.put("table-name", TABLE_NAME);
        options.put("table-list", TABLE_LIST);
        options.put("hostname", HOSTNAME);
        options.put("port", String.valueOf(PORT));
        options.put("logproxy.client.id", LOG_PROXY_CLIENT_ID);
        options.put("rootserver-list", RS_LIST);
        DynamicTableSource actualSource = createTableSource(SCHEMA, options);

        OceanBaseTableSource expectedSource =
                new OceanBaseTableSource(
                        SCHEMA,
                        StartupMode.INITIAL,
                        USERNAME,
                        PASSWORD,
                        TENANT_NAME,
                        DATABASE_NAME,
                        TABLE_NAME,
                        TABLE_LIST,
                        SERVER_TIME_ZONE,
                        Duration.parse("PT" + CONNECT_TIMEOUT),
                        "127.0.0.1",
                        2881,
                        LOG_PROXY_HOST,
                        LOG_PROXY_PORT,
                        LOG_PROXY_CLIENT_ID,
                        null,
                        RS_LIST,
                        null,
                        WORKING_MODE);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testMetadataColumns() {
        Map<String, String> options = getRequiredOptions();
        options.put("database-name", DATABASE_NAME);
        options.put("table-name", TABLE_NAME);
        options.put("table-list", TABLE_LIST);
        options.put("rootserver-list", RS_LIST);

        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
        OceanBaseTableSource oceanBaseTableSource = (OceanBaseTableSource) actualSource;
        oceanBaseTableSource.applyReadableMetadata(
                Arrays.asList("op_ts", "tenant_name", "database_name", "table_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = oceanBaseTableSource.copy();

        OceanBaseTableSource expectedSource =
                new OceanBaseTableSource(
                        SCHEMA_WITH_METADATA,
                        StartupMode.LATEST_OFFSET,
                        USERNAME,
                        PASSWORD,
                        TENANT_NAME,
                        DATABASE_NAME,
                        TABLE_NAME,
                        TABLE_LIST,
                        SERVER_TIME_ZONE,
                        Duration.parse("PT" + CONNECT_TIMEOUT),
                        null,
                        null,
                        LOG_PROXY_HOST,
                        LOG_PROXY_PORT,
                        null,
                        null,
                        RS_LIST,
                        null,
                        WORKING_MODE);
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys =
                Arrays.asList("op_ts", "tenant_name", "database_name", "table_name");

        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        try {
            Map<String, String> properties = getRequiredOptions();
            properties.put("unknown", "abc");

            createTableSource(SCHEMA, properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }
    }

    private Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "oceanbase-cdc");
        options.put("scan.startup.mode", STARTUP_MODE);
        options.put("username", USERNAME);
        options.put("password", PASSWORD);
        options.put("tenant-name", TENANT_NAME);
        options.put("logproxy.host", LOG_PROXY_HOST);
        options.put("logproxy.port", String.valueOf(LOG_PROXY_PORT));
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
                OceanBaseTableFactoryTest.class.getClassLoader(),
                false);
    }
}
