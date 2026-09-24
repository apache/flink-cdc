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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HadoopConfUtils;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.operator.MonitorSource;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Attaches Iceberg's polling maintenance topology to the CDC execution environment. */
@Internal
public final class TableMaintenanceTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceTopology.class);

    private TableMaintenanceTopology() {}

    /** Builds native maintenance; apply the returned adapter after generating the StreamGraph. */
    public static MaintenanceGraphAdapter append(
            StreamExecutionEnvironment env,
            Map<String, String> catalogOptions,
            Map<String, String> hadoopConfOptions,
            MaintenanceOptions options) {
        MaintenanceGraphAdapter adapter = new MaintenanceGraphAdapter();
        append(env, catalogOptions, hadoopConfOptions, options, adapter);
        return adapter;
    }

    public static void append(
            StreamExecutionEnvironment env,
            Map<String, String> catalogOptions,
            Map<String, String> hadoopConfOptions,
            MaintenanceOptions options,
            MaintenanceGraphAdapter adapter) {
        adapter.startTopology();
        if (!options.get(MaintenanceOptions.ENABLED)) {
            return;
        }
        checkArgument(
                !options.requiresTableDiscovery(),
                "Maintenance targets must be discovered before building the topology. "
                        + "Use the CDC pipeline composer or configure sink.maintenance.tables explicitly.");
        checkArgument(
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        != RuntimeExecutionMode.BATCH,
                "Iceberg table maintenance requires streaming execution mode.");
        checkArgument(
                env.getCheckpointConfig().isCheckpointingEnabled(),
                "Iceberg table maintenance requires checkpointing to be enabled.");

        CatalogLoader catalogLoader =
                new MaintenanceCatalogLoader(catalogOptions, hadoopConfOptions);
        validateExistingTargets(catalogLoader, options.tables());
        Map<String, String> jdbcProperties = options.jdbcProperties();
        if (options.get(MaintenanceOptions.JDBC_INIT_LOCK_TABLES)) {
            // Initialize once on submission, before multiple operators can race to create the
            // table.
            try (JdbcLockFactory initializer =
                    new JdbcLockFactory(
                            options.get(MaintenanceOptions.JDBC_URI),
                            "cdc-maintenance-initialization",
                            jdbcProperties)) {
                initializer.open();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close maintenance lock initializer", e);
            }
            jdbcProperties.put(JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, "false");
        }
        DataStream<Tuple2<String, TableChange>> allChanges = null;
        for (TableId tableId : options.tables()) {
            String uid = uidSuffix(options, tableId);
            DeferredTableLoader loader =
                    new DeferredTableLoader(
                            tableLoader(catalogLoader, tableId), tableName(tableId));
            DataStream<Tuple2<String, TableChange>> changes =
                    env.fromSource(
                                    new ClosingMonitorSource<>(
                                            new MonitorSource(
                                                    loader,
                                                    RateLimiterStrategy.perSecond(
                                                            1.0
                                                                    / options.get(
                                                                                    MaintenanceOptions
                                                                                            .RATE_LIMIT)
                                                                            .getSeconds()),
                                                    options.get(MaintenanceOptions.MAX_READ_BACK)),
                                            loader),
                                    WatermarkStrategy.noWatermarks(),
                                    "Monitor source for " + tableName(tableId),
                                    TypeInformation.of(TableChange.class))
                            .uid("Monitor source for " + uid)
                            .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP))
                            .forceNonParallel()
                            .map(change -> Tuple2.of(tableId.identifier(), change))
                            .returns(
                                    TypeInformation.of(
                                            new TypeHint<Tuple2<String, TableChange>>() {}))
                            .name("Identify maintenance target")
                            .uid("table-change-id-" + uid)
                            .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP))
                            .forceNonParallel();
            allChanges = allChanges == null ? changes : allChanges.union(changes);
        }
        SingleOutputStreamOperator<Void> readyChanges =
                allChanges
                        .process(new TargetReadiness(catalogLoader, options))
                        .name("Wait for CDC target tables")
                        .uid("table-readiness-" + options.get(MaintenanceOptions.UID_PREFIX))
                        .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP))
                        .forceNonParallel();
        for (TableId tableId : options.tables()) {
            appendTable(
                    readyChanges.getSideOutput(TargetReadiness.outputTag(tableId.identifier())),
                    catalogLoader,
                    tableId,
                    jdbcProperties,
                    options,
                    adapter);
        }
    }

    private static void validateExistingTargets(CatalogLoader catalogLoader, List<TableId> tables) {
        Catalog catalog = catalogLoader.loadCatalog();
        try (Closeable ignored = catalog instanceof Closeable ? (Closeable) catalog : null) {
            Map<UUID, TableId> resolvedTargets = new HashMap<>();
            for (TableId tableId : tables) {
                try {
                    Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
                    TableId previous = resolvedTargets.put(table.uuid(), tableId);
                    checkArgument(
                            previous == null,
                            "Maintenance targets %s and %s refer to the same Iceberg table.",
                            previous,
                            tableId);
                } catch (NoSuchTableException e) {
                    LOG.debug("Maintenance target {} will be created by CDC", tableId);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            "Cannot prepare maintenance target " + tableId, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close maintenance catalog", e);
        }
    }

    private static void appendTable(
            DataStream<TableChange> changes,
            CatalogLoader catalogLoader,
            TableId tableId,
            Map<String, String> jdbcProperties,
            MaintenanceOptions options,
            MaintenanceGraphAdapter adapter) {
        String uid = uidSuffix(options, tableId);
        DeferredTableLoader loader =
                new DeferredTableLoader(tableLoader(catalogLoader, tableId), tableName(tableId));
        TaskRegistration registration =
                new TaskRegistration(
                        adapter, loader, options.get(MaintenanceOptions.DELETE_BATCH_SIZE));
        JdbcLockFactory lockFactory =
                new JdbcLockFactory(
                        options.get(MaintenanceOptions.JDBC_URI),
                        UUID.nameUUIDFromBytes(uid.getBytes(StandardCharsets.UTF_8)).toString(),
                        jdbcProperties);
        TableMaintenance.Builder maintenance =
                TableMaintenance.forChangeStream(changes, loader, lockFactory)
                        .uidSuffix(uid)
                        .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP))
                        .parallelism(options.get(MaintenanceOptions.PARALLELISM))
                        .rateLimit(options.get(MaintenanceOptions.RATE_LIMIT))
                        .lockCheckDelay(options.get(MaintenanceOptions.LOCK_CHECK_DELAY));
        if (options.get(MaintenanceOptions.REWRITE_ENABLED)) {
            RewriteDataFiles.Builder rewrite =
                    new RewriteDataFiles.Builder() {
                        @Override
                        protected TableLoader tableLoader() {
                            return registration.register(
                                    super.tableLoader(),
                                    uidSuffix(),
                                    tableName(),
                                    taskName(),
                                    index());
                        }
                    };
            rewrite.scheduleOnInterval(options.get(MaintenanceOptions.REWRITE_INTERVAL))
                    .targetFileSizeBytes(options.get(MaintenanceOptions.REWRITE_TARGET_FILE_SIZE))
                    .minInputFiles(options.get(MaintenanceOptions.REWRITE_MIN_INPUT_FILES))
                    .deleteFileThreshold(
                            options.get(MaintenanceOptions.REWRITE_DELETE_FILE_THRESHOLD))
                    .maxRewriteBytes(options.get(MaintenanceOptions.REWRITE_MAX_BYTES))
                    .maxFileGroupSizeBytes(
                            Math.min(
                                    options.get(MaintenanceOptions.REWRITE_MAX_BYTES),
                                    SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT));
            Integer commits = options.get(MaintenanceOptions.REWRITE_COMMIT_COUNT);
            if (commits != null) {
                rewrite.scheduleOnCommitCount(commits);
            }
            Integer files = options.get(MaintenanceOptions.REWRITE_DATA_FILE_COUNT);
            if (files != null) {
                rewrite.scheduleOnDataFileCount(files);
            }
            maintenance.add(rewrite);
        }
        if (options.get(MaintenanceOptions.EXPIRE_ENABLED)) {
            ExpireSnapshots.Builder expire =
                    new ExpireSnapshots.Builder() {
                        @Override
                        protected TableLoader tableLoader() {
                            return registration.register(
                                    super.tableLoader(),
                                    uidSuffix(),
                                    tableName(),
                                    taskName(),
                                    index());
                        }
                    };
            expire.scheduleOnInterval(options.get(MaintenanceOptions.EXPIRE_INTERVAL))
                    .maxSnapshotAge(options.get(MaintenanceOptions.EXPIRE_MAX_AGE))
                    .retainLast(options.get(MaintenanceOptions.EXPIRE_RETAIN_LAST))
                    .deleteBatchSize(options.get(MaintenanceOptions.DELETE_BATCH_SIZE));
            Integer commits = options.get(MaintenanceOptions.EXPIRE_COMMIT_COUNT);
            if (commits != null) {
                expire.scheduleOnCommitCount(commits);
            }
            maintenance.add(expire);
        }
        if (options.get(MaintenanceOptions.ORPHAN_ENABLED)) {
            DeleteOrphanFiles.Builder orphan =
                    new RefreshingDeleteOrphanFilesBuilder() {
                        @Override
                        protected TableLoader tableLoader() {
                            return registration.register(
                                    super.tableLoader(),
                                    uidSuffix(),
                                    tableName(),
                                    taskName(),
                                    index());
                        }
                    };
            orphan.scheduleOnInterval(options.get(MaintenanceOptions.ORPHAN_INTERVAL))
                    // Use the table's configured FileIO instead of a fresh Hadoop configuration.
                    .usePrefixListing(true)
                    .minAge(options.get(MaintenanceOptions.ORPHAN_MIN_AGE))
                    .deleteBatchSize(options.get(MaintenanceOptions.DELETE_BATCH_SIZE));
            maintenance.add(orphan);
        }
        try {
            maintenance.append();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot build Iceberg maintenance for " + tableId, e);
        }
    }

    private static String tableName(TableId tableId) {
        return "cdc-iceberg-maintenance." + tableId.identifier();
    }

    static String uidSuffix(MaintenanceOptions options, TableId tableId) {
        // Iceberg stores trigger counters by task index. A different task set needs fresh state.
        String stateIdentity =
                tableId.identifier()
                        + ":"
                        + options.get(MaintenanceOptions.REWRITE_ENABLED)
                        + ":"
                        + options.get(MaintenanceOptions.EXPIRE_ENABLED)
                        + ":"
                        + options.get(MaintenanceOptions.ORPHAN_ENABLED);
        return options.get(MaintenanceOptions.UID_PREFIX)
                + "-"
                + UUID.nameUUIDFromBytes(stateIdentity.getBytes(StandardCharsets.UTF_8));
    }

    private static TableLoader tableLoader(CatalogLoader catalogLoader, TableId tableId) {
        return TableLoader.fromCatalog(catalogLoader, TableIdentifier.parse(tableId.identifier()));
    }

    /** Shares submission-time registration across native maintenance builders. */
    private static final class TaskRegistration {
        private final MaintenanceGraphAdapter adapter;
        private final DeferredTableLoader deletionLoader;
        private final int deleteBatchSize;

        private TaskRegistration(
                MaintenanceGraphAdapter adapter,
                DeferredTableLoader deletionLoader,
                int deleteBatchSize) {
            this.adapter = adapter;
            this.deletionLoader = deletionLoader;
            this.deleteBatchSize = deleteBatchSize;
        }

        private TableLoader register(
                TableLoader bound, String uid, String tableName, String taskName, int index) {
            adapter.register(
                    uid, tableName, taskName, index, bound, deletionLoader, deleteBatchSize);
            return bound;
        }
    }

    /** Uses the same catalog resolution and Hadoop options as the CDC writer. */
    private static final class MaintenanceCatalogLoader implements CatalogLoader {
        private static final long serialVersionUID = 1L;
        private final Map<String, String> catalogOptions;
        private final Map<String, String> hadoopConfOptions;

        private MaintenanceCatalogLoader(
                Map<String, String> catalogOptions, Map<String, String> hadoopConfOptions) {
            this.catalogOptions = new HashMap<>(catalogOptions);
            this.hadoopConfOptions = new HashMap<>(hadoopConfOptions);
        }

        @Override
        public Catalog loadCatalog() {
            return CatalogUtil.buildIcebergCatalog(
                    "cdc-iceberg-maintenance",
                    catalogOptions,
                    HadoopConfUtils.createConfiguration(hadoopConfOptions));
        }

        @Override
        public CatalogLoader clone() {
            return new MaintenanceCatalogLoader(catalogOptions, hadoopConfOptions);
        }
    }
}
