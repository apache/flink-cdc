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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils;
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SRV_SCHEME;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_NO_CURSOR_TIMEOUT;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCHEME;
import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertProducedTypeOfSourceFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MongoDBTableSource} created by {@link MongoDBTableSourceFactory}. */
class MongoDBTableFactoryTest {
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
                                    "_database_name", DataTypes.STRING(), "database_name", true),
                            Column.metadata("_row_kind", DataTypes.STRING(), "row_kind", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("_id")));

    private static final String MY_HOSTS = "localhost:27017,localhost:27018";
    private static final String USER = "flinkuser";
    private static final String PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final ZoneId LOCAL_TIME_ZONE = ZoneId.systemDefault();
    private static final int BATCH_SIZE_DEFAULT = BATCH_SIZE.defaultValue();
    private static final int POLL_MAX_BATCH_SIZE_DEFAULT = POLL_MAX_BATCH_SIZE.defaultValue();
    private static final int POLL_AWAIT_TIME_MILLIS_DEFAULT = POLL_AWAIT_TIME_MILLIS.defaultValue();
    private static final int HEARTBEAT_INTERVAL_MILLIS_DEFAULT =
            HEARTBEAT_INTERVAL_MILLIS.defaultValue();
    private static final boolean SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue();
    private static final int SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();
    private static final int SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES.defaultValue();
    private static final int CHUNK_META_GROUP_SIZE_DEFAULT = CHUNK_META_GROUP_SIZE.defaultValue();
    private static final boolean SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT =
            SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue();

    private static final boolean FULL_DOCUMENT_PRE_POST_IMAGE_ENABLED_DEFAULT =
            FULL_DOCUMENT_PRE_POST_IMAGE.defaultValue();

    private static final boolean SCAN_NO_CURSOR_TIMEOUT_DEFAULT =
            SCAN_NO_CURSOR_TIMEOUT.defaultValue();
    private static final boolean SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP_DEFAULT =
            SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue();

    private static final boolean SCAN_NEWLY_ADDED_TABLE_ENABLED_DEFAULT =
            SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue();

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        SCHEMA,
                        SCHEME.defaultValue(),
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        StartupOptions.initial(),
                        null,
                        null,
                        null,
                        BATCH_SIZE_DEFAULT,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        HEARTBEAT_INTERVAL_MILLIS_DEFAULT,
                        LOCAL_TIME_ZONE,
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT,
                        CHUNK_META_GROUP_SIZE_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES_DEFAULT,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        FULL_DOCUMENT_PRE_POST_IMAGE_ENABLED_DEFAULT,
                        SCAN_NO_CURSOR_TIMEOUT_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP_DEFAULT,
                        SCAN_NEWLY_ADDED_TABLE_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("scheme", MONGODB_SRV_SCHEME);
        options.put("connection.options", "replicaSet=test&connectTimeoutMS=300000");
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp-millis", "1667232000000");
        options.put("batch.size", "101");
        options.put("poll.max.batch.size", "102");
        options.put("poll.await.time.ms", "103");
        options.put("heartbeat.interval.ms", "104");
        options.put("scan.incremental.snapshot.enabled", "true");
        options.put("chunk-meta.group.size", "1001");
        options.put("scan.incremental.snapshot.chunk.size.mb", "10");
        options.put("scan.incremental.snapshot.chunk.samples", "10");
        options.put("scan.incremental.close-idle-reader.enabled", "true");
        options.put("scan.incremental.snapshot.backfill.skip", "true");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("scan.full-changelog", "true");
        options.put("scan.cursor.no-timeout", "false");
        options.put("scan.incremental.snapshot.unbounded-chunk-first.enabled", "true");

        DynamicTableSource actualSource = createTableSource(SCHEMA, options);

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        SCHEMA,
                        MONGODB_SRV_SCHEME,
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        "replicaSet=test&connectTimeoutMS=300000",
                        StartupOptions.timestamp(1667232000000L),
                        null,
                        null,
                        null,
                        101,
                        102,
                        103,
                        104,
                        LOCAL_TIME_ZONE,
                        true,
                        1001,
                        10,
                        10,
                        true,
                        true,
                        false,
                        true,
                        true,
                        true);
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        MongoDBTableSource mongoDBSource = (MongoDBTableSource) actualSource;
        mongoDBSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name", "row_kind"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = mongoDBSource.copy();

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        ResolvedSchemaUtils.getPhysicalSchema(SCHEMA_WITH_METADATA),
                        SCHEME.defaultValue(),
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        StartupOptions.initial(),
                        null,
                        null,
                        null,
                        BATCH_SIZE_DEFAULT,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        HEARTBEAT_INTERVAL_MILLIS_DEFAULT,
                        LOCAL_TIME_ZONE,
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED_DEFAULT,
                        CHUNK_META_GROUP_SIZE_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES_DEFAULT,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        FULL_DOCUMENT_PRE_POST_IMAGE_ENABLED_DEFAULT,
                        SCAN_NO_CURSOR_TIMEOUT_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP_DEFAULT,
                        SCAN_NEWLY_ADDED_TABLE_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue());

        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys = Arrays.asList("op_ts", "database_name", "row_kind");

        Assertions.assertThat(actualSource).isEqualTo(expectedSource);

        ScanTableSource.ScanRuntimeProvider provider =
                mongoDBSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        DebeziumSourceFunction<RowData> debeziumSourceFunction =
                (DebeziumSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();
        assertProducedTypeOfSourceFunction(debeziumSourceFunction, expectedSource.producedDataType);
    }

    @Test
    void testValidation() {
        // validate unsupported option
        Assertions.assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("unknown", "abc");
                            createTableSource(SCHEMA, properties);
                        })
                .hasStackTraceContaining("Unsupported options:\n\nunknown");
    }

    @Test
    public void testCopyExistingPipelineConflictWithIncrementalSnapshotMode() {
        // test with 'initial.snapshotting.pipeline' configuration
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put(
                                    "initial.snapshotting.pipeline",
                                    "[{\"$match\": {\"closed\": \"false\"}}]");
                            createTableSource(SCHEMA, properties);
                        })
                .rootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The initial.snapshotting.*/copy.existing.* config only applies to "
                                + "Debezium mode, not incremental snapshot mode");

        // test with 'initial.snapshotting.max.threads' configuration
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("initial.snapshotting.max.threads", "20");
                            createTableSource(SCHEMA, properties);
                        })
                .rootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The initial.snapshotting.*/copy.existing.* config only applies to "
                                + "Debezium mode, not incremental snapshot mode");

        // test with 'initial.snapshotting.queue.size' configuration
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("scan.incremental.snapshot.enabled", "true");
                            properties.put("initial.snapshotting.queue.size", "20480");
                            createTableSource(SCHEMA, properties);
                        })
                .rootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The initial.snapshotting.*/copy.existing.* config only applies to "
                                + "Debezium mode, not incremental snapshot mode");
    }

    @Test
    public void testCopyExistingPipelineInDebeziumMode() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "false");
        properties.put("initial.snapshotting.pipeline", "[{\"$match\": {\"closed\": \"false\"}}]");
        properties.put("initial.snapshotting.max.threads", "20");
        properties.put("initial.snapshotting.queue.size", "20480");
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);

        MongoDBTableSource expectedSource =
                new MongoDBTableSource(
                        SCHEMA,
                        SCHEME.defaultValue(),
                        MY_HOSTS,
                        USER,
                        PASSWORD,
                        MY_DATABASE,
                        MY_TABLE,
                        null,
                        StartupOptions.initial(),
                        20480,
                        20,
                        "[{\"$match\": {\"closed\": \"false\"}}]",
                        BATCH_SIZE_DEFAULT,
                        POLL_MAX_BATCH_SIZE_DEFAULT,
                        POLL_AWAIT_TIME_MILLIS_DEFAULT,
                        HEARTBEAT_INTERVAL_MILLIS_DEFAULT,
                        LOCAL_TIME_ZONE,
                        false,
                        CHUNK_META_GROUP_SIZE_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES_DEFAULT,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED_DEFAULT,
                        FULL_DOCUMENT_PRE_POST_IMAGE_ENABLED_DEFAULT,
                        SCAN_NO_CURSOR_TIMEOUT_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP_DEFAULT,
                        SCAN_NEWLY_ADDED_TABLE_ENABLED_DEFAULT,
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue());
        Assertions.assertThat(actualSource).isEqualTo(expectedSource);
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
