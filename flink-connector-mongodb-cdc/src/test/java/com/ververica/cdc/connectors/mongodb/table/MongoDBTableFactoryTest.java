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

package com.ververica.cdc.connectors.mongodb.table;

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
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.utils.ResolvedSchemaUtils;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COPY_EXISTING;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertProducedTypeOfSourceFunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link MongoDBTableSource} created by {@link MongoDBTableSourceFactory}. */
public class MongoDBTableFactoryTest {
    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("_id", DataTypes.STRING().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("_id")));

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("_id", DataTypes.STRING().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3)),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata(
                                    "_database_name", DataTypes.STRING(), "database_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("_id")));

    private static final String MY_HOSTS = "localhost:27017,localhost:27018";
    private static final String USER = "flinkuser";
    private static final String PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final ZoneId LOCAL_TIME_ZONE = ZoneId.systemDefault();
    private static final Boolean COPY_EXISTING_DEFAULT = COPY_EXISTING.defaultValue();
    private static final int BATCH_SIZE_DEFAULT = BATCH_SIZE.defaultValue();
    private static final int POLL_MAX_BATCH_SIZE_DEFAULT = POLL_MAX_BATCH_SIZE.defaultValue();
    private static final int POLL_AWAIT_TIME_MILLIS_DEFAULT = POLL_AWAIT_TIME_MILLIS.defaultValue();
    private static final int HEARTBEAT_INTERVAL_MILLIS_DEFAULT =
            HEARTBEAT_INTERVAL_MILLIS.defaultValue();
    private static final boolean SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue();
    private static final int SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();
    private static final int CHUNK_META_GROUP_SIZE_DEFAULT = CHUNK_META_GROUP_SIZE.defaultValue();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        SCHEMA,
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        COPY_EXISTING_DEFAULT,
                        null,
                        BATCH_SIZE_DEFAULT,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        HEARTBEAT_INTERVAL_MILLIS_DEFAULT,
                        LOCAL_TIME_ZONE,
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT,
                        CHUNK_META_GROUP_SIZE_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("connection.options", "replicaSet=test&connectTimeoutMS=300000");
        options.put("copy.existing", "false");
        options.put("copy.existing.queue.size", "100");
        options.put("batch.size", "101");
        options.put("poll.max.batch.size", "102");
        options.put("poll.await.time.ms", "103");
        options.put("heartbeat.interval.ms", "104");
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("chunk-meta.group.size", "1001");
        options.put("scan.incremental.snapshot.chunk.size.mb", "10");
        DynamicTableSource actualSource = createTableSource(SCHEMA, options);

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        SCHEMA,
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        "replicaSet=test&connectTimeoutMS=300000",
                        false,
                        100,
                        101,
                        102,
                        103,
                        104,
                        LOCAL_TIME_ZONE,
                        true,
                        1001,
                        10);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        MongoDBTableSource mongoDBSource = (MongoDBTableSource) actualSource;
        mongoDBSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = mongoDBSource.copy();

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        ResolvedSchemaUtils.getPhysicalSchema(SCHEMA_WITH_METADATA),
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        COPY_EXISTING_DEFAULT,
                        null,
                        BATCH_SIZE_DEFAULT,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        HEARTBEAT_INTERVAL_MILLIS_DEFAULT,
                        LOCAL_TIME_ZONE,
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT,
                        CHUNK_META_GROUP_SIZE_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT);

        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys = Arrays.asList("op_ts", "database_name");

        assertEquals(expectedSource, actualSource);

        ScanTableSource.ScanRuntimeProvider provider =
                mongoDBSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        DebeziumSourceFunction<RowData> debeziumSourceFunction =
                (DebeziumSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();
        assertProducedTypeOfSourceFunction(debeziumSourceFunction, expectedSource.producedDataType);
    }

    @Test
    public void testValidation() {
        // validate unsupported option
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("unknown", "abc");

            createTableSource(SCHEMA, properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb-cdc");
        options.put("hosts", MY_HOSTS);
        options.put("username", USER);
        options.put("password", PASSWORD);
        options.put("database", MY_DATABASE);
        options.put("collection", MY_TABLE);
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
                MongoDBTableFactoryTest.class.getClassLoader(),
                false);
    }
}
