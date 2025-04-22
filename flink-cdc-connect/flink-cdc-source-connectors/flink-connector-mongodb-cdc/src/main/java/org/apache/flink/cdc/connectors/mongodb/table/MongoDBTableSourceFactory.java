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
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COLLECTION;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.CONNECTION_OPTIONS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HOSTS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.INITIAL_SNAPSHOTTING_MAX_THREADS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.INITIAL_SNAPSHOTTING_PIPELINE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.INITIAL_SNAPSHOTTING_QUEUE_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_NO_CURSOR_TIMEOUT;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCHEME;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating configured instance of {@link MongoDBTableSource}. */
public class MongoDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mongodb-cdc";

    private static final String DOCUMENT_ID_FIELD = "_id";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String scheme = config.get(SCHEME);
        String hosts = config.get(HOSTS);
        String connectionOptions = config.getOptional(CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);

        String database = config.getOptional(DATABASE).orElse(null);
        String collection = config.getOptional(COLLECTION).orElse(null);

        Integer batchSize = config.get(BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis = config.get(HEARTBEAT_INTERVAL_MILLIS);

        StartupOptions startupOptions = getStartupOptions(config);
        Integer initialSnapshottingQueueSize =
                config.getOptional(INITIAL_SNAPSHOTTING_QUEUE_SIZE).orElse(null);
        Integer initialSnapshottingMaxThreads =
                config.getOptional(INITIAL_SNAPSHOTTING_MAX_THREADS).orElse(null);
        String initialSnapshottingPipeline =
                config.getOptional(INITIAL_SNAPSHOTTING_PIPELINE).orElse(null);

        String zoneId = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        ZoneId localTimeZone =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneId);

        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);

        // The initial.snapshotting.pipeline related config is only used in Debezium mode and
        // cannot be used in incremental snapshot mode because the semantic is inconsistent.
        // The reason is that in snapshot phase of incremental snapshot mode, the oplog
        // will be backfilled after each snapshot to compensate for changes, but the pipeline
        // operations in initial.snapshotting.pipeline are not applied to the backfill oplog,
        // which means the semantic of this config is inconsistent.
        checkArgument(
                !(enableParallelRead
                        && (initialSnapshottingPipeline != null
                                || initialSnapshottingMaxThreads != null
                                || initialSnapshottingQueueSize != null)),
                "The initial.snapshotting.*/copy.existing.* config only applies to Debezium mode, "
                        + "not incremental snapshot mode");

        boolean enableCloseIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean assignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        int splitSizeMB = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int samplesPerChunk = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);

        boolean enableFullDocumentPrePostImage =
                config.getOptional(FULL_DOCUMENT_PRE_POST_IMAGE).orElse(false);

        boolean noCursorTimeout = config.getOptional(SCAN_NO_CURSOR_TIMEOUT).orElse(true);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        checkArgument(physicalSchema.getPrimaryKey().isPresent(), "Primary key must be present");
        checkPrimaryKey(physicalSchema.getPrimaryKey().get(), "Primary key must be _id field");

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return new MongoDBTableSource(
                physicalSchema,
                scheme,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                startupOptions,
                initialSnapshottingQueueSize,
                initialSnapshottingMaxThreads,
                initialSnapshottingPipeline,
                batchSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone,
                enableParallelRead,
                splitMetaGroupSize,
                splitSizeMB,
                samplesPerChunk,
                enableCloseIdleReaders,
                enableFullDocumentPrePostImage,
                noCursorTimeout,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }

    private void checkPrimaryKey(UniqueConstraint pk, String message) {
        checkArgument(
                pk.getColumns().size() == 1 && pk.getColumns().contains(DOCUMENT_ID_FIELD),
                message);
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        checkNotNull(
                                config.get(SCAN_STARTUP_TIMESTAMP_MILLIS),
                                String.format(
                                        "To use timestamp startup mode, the startup timestamp millis '%s' must be set.",
                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key())));
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECTION_OPTIONS);
        options.add(DATABASE);
        options.add(COLLECTION);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(INITIAL_SNAPSHOTTING_QUEUE_SIZE);
        options.add(INITIAL_SNAPSHOTTING_MAX_THREADS);
        options.add(INITIAL_SNAPSHOTTING_PIPELINE);
        options.add(BATCH_SIZE);
        options.add(POLL_MAX_BATCH_SIZE);
        options.add(POLL_AWAIT_TIME_MILLIS);
        options.add(HEARTBEAT_INTERVAL_MILLIS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(FULL_DOCUMENT_PRE_POST_IMAGE);
        options.add(SCAN_NO_CURSOR_TIMEOUT);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        return options;
    }
}
