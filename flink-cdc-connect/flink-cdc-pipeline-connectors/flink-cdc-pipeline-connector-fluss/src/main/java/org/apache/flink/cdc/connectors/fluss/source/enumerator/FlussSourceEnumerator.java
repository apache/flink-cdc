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

package org.apache.flink.cdc.connectors.fluss.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.common.source.discover.TableDiscovererFactory;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussDefaultDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.initializer.SnapshotOffsetsInitializer;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The enumerator for Fluss source. It discovers tables using {@link TableDiscoverer}, queries their
 * metadata (schema, bucket count, partitions), and generates {@link FlussSplitBase}s for each
 * table-bucket pair, assigning them to readers in a round-robin fashion.
 *
 * <p>The enumeration follows a four-phase pattern:
 *
 * <ol>
 *   <li>{@link #getSubscribedTableBuckets()} — discovers subscribed tables and enumerates all
 *       table-buckets including partitions (async).
 *   <li>{@link #checkTableBucketChanges} — compares discovered table-buckets with already-assigned
 *       ones and triggers split creation for new table-buckets (callback).
 *   <li>{@link #initPendingBucketSplits} — resolves starting offsets and creates splits for new
 *       table-buckets (async).
 *   <li>{@link #handleTableBucketChanges} — marks physical table paths as assigned and distributes
 *       splits to readers (callback).
 * </ol>
 *
 * <p>Tracking is done at {@link PhysicalTablePath} granularity (i.e. tablePath + partitionName), so
 * newly created partitions of an already-known table will be discovered and assigned.
 *
 * <p>The starting offsets for each bucket are resolved via the {@link OffsetsInitializer}, which
 * supports earliest, latest, and timestamp-based initialization strategies.
 */
public class FlussSourceEnumerator
        implements SplitEnumerator<FlussSplitBase, FlussSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceEnumerator.class);

    private final SplitEnumeratorContext<FlussSplitBase> context;
    private final TableDiscoverer discoverer;
    private final org.apache.fluss.config.Configuration flussConfig;
    private final Configuration sourceConfig;
    private final OffsetsInitializer offsetsInitializer;
    private final long scanDiscoveryIntervalMs;

    private final Set<PhysicalTablePath> assignedPhysicalTablePaths;
    private final Map<Integer, Set<FlussSplitBase>> pendingPartitionSplitAssignment;

    private transient Connection connection;
    private transient Admin admin;

    public FlussSourceEnumerator(
            SplitEnumeratorContext<FlussSplitBase> context,
            TableDiscoverer discoverer,
            org.apache.fluss.config.Configuration flussConfig,
            Configuration sourceConfig,
            OffsetsInitializer offsetsInitializer,
            long scanDiscoveryIntervalMs,
            Set<PhysicalTablePath> assignedPhysicalTablePaths) {
        this.context = context;
        this.discoverer = discoverer;
        this.flussConfig = flussConfig;
        this.sourceConfig = sourceConfig;
        this.offsetsInitializer = offsetsInitializer;
        this.scanDiscoveryIntervalMs = scanDiscoveryIntervalMs;
        this.assignedPhysicalTablePaths = assignedPhysicalTablePaths;
        this.pendingPartitionSplitAssignment = new HashMap<>();
    }

    public FlussSourceEnumerator(
            SplitEnumeratorContext<FlussSplitBase> context,
            TableDiscoverer discoverer,
            org.apache.fluss.config.Configuration flussConfig,
            Configuration sourceConfig,
            OffsetsInitializer offsetsInitializer,
            long scanDiscoveryIntervalMs,
            FlussSourceEnumState restoredState) {
        this(
                context,
                discoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                scanDiscoveryIntervalMs,
                restoredState.getAssignedPhysicalTablePaths());
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();

        // Open the discoverer with the full source configuration
        try {
            discoverer.open(
                    TableDiscovererFactory.createContext(
                            sourceConfig, Thread.currentThread().getContextClassLoader()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to open TableDiscoverer", e);
        }

        if (scanDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting Fluss source enumerator with a discovery interval of {} ms.",
                    scanDiscoveryIntervalMs);
            context.callAsync(
                    this::getSubscribedTableBuckets,
                    this::checkTableBucketChanges,
                    0,
                    scanDiscoveryIntervalMs);
        } else {
            LOG.info("Starting Fluss source enumerator without periodic discovery.");
            context.callAsync(this::getSubscribedTableBuckets, this::checkTableBucketChanges);
        }
    }

    // -------------------------------------------------------------------------
    //  Phase 1: Discover subscribed table-buckets (runs async)
    // -------------------------------------------------------------------------

    /**
     * Discovers all subscribed tables via the {@link TableDiscoverer}, then queries their metadata
     * (bucket count, partitions) and enumerates every individual table-bucket. For partitioned
     * tables, each partition contributes its own set of buckets.
     *
     * @return the full list of discovered table-bucket entries.
     */
    private List<TableBucketInfo> getSubscribedTableBuckets() throws Exception {
        List<TableBucketInfo> allBuckets = new ArrayList<>();
        Set<TableId> discoveredTableIds = discoverer.discover();
        Set<TablePath> subscribedPaths =
                discoveredTableIds.stream()
                        .map(FlussDefaultDiscoverer::toTablePath)
                        .collect(Collectors.toCollection(java.util.LinkedHashSet::new));

        for (TablePath tablePath : subscribedPaths) {
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            int numBuckets = tableInfo.getNumBuckets();
            long tableId = tableInfo.getTableId();

            boolean hasPrimaryKey = tableInfo.hasPrimaryKey();

            if (tableInfo.isPartitioned()) {
                List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
                for (PartitionInfo partitionInfo : partitions) {
                    long partitionId = partitionInfo.getPartitionId();
                    String partitionName = partitionInfo.getPartitionName();
                    PhysicalTablePath physicalTablePath =
                            PhysicalTablePath.of(tablePath, partitionName);
                    for (int bucket = 0; bucket < numBuckets; bucket++) {
                        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                        allBuckets.add(
                                new TableBucketInfo(physicalTablePath, tableBucket, hasPrimaryKey));
                    }
                }
            } else {
                PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    TableBucket tableBucket = new TableBucket(tableId, bucket);
                    allBuckets.add(
                            new TableBucketInfo(physicalTablePath, tableBucket, hasPrimaryKey));
                }
            }
        }
        return allBuckets;
    }

    // -------------------------------------------------------------------------
    //  Phase 2: Check for table-bucket changes (callback)
    // -------------------------------------------------------------------------

    /**
     * Compares the discovered table-buckets against already-assigned {@link PhysicalTablePath}s and
     * triggers split creation for newly discovered table-buckets.
     */
    private void checkTableBucketChanges(List<TableBucketInfo> allBuckets, Throwable error) {
        if (error != null) {
            throw new FlinkRuntimeException("Failed to discover subscribed table-buckets.", error);
        }

        List<TableBucketInfo> newBuckets = new ArrayList<>();
        for (TableBucketInfo info : allBuckets) {
            if (!assignedPhysicalTablePaths.contains(info.physicalTablePath)) {
                newBuckets.add(info);
            }
        }

        if (newBuckets.isEmpty()) {
            LOG.debug("No new table-buckets discovered.");
            return;
        }

        LOG.info("Discovered {} new table-bucket(s) to initialize.", newBuckets.size());
        context.callAsync(
                () -> initPendingBucketSplits(newBuckets), this::handleTableBucketChanges);
    }

    // -------------------------------------------------------------------------
    //  Phase 3: Create pending splits for new table-buckets (runs async)
    // -------------------------------------------------------------------------

    /**
     * Groups the new table-buckets by {@link TablePath} (for the {@link BucketOffsetsRetrieverImpl}
     * instance) and then by partition name (for batch offset resolution via the {@link
     * OffsetsInitializer}), and creates {@link FlussSplitBase} instances.
     *
     * <p>For primary key tables with {@link SnapshotOffsetsInitializer} ("full" startup mode), KV
     * snapshots are retrieved: buckets with a snapshot get a {@link FlussHybridSnapshotLogSplit},
     * buckets without a snapshot fall back to a {@link FlussLogSplit}.
     */
    private List<FlussSplitBase> initPendingBucketSplits(List<TableBucketInfo> newBuckets)
            throws Exception {
        List<FlussSplitBase> newSplits = new ArrayList<>();

        // Group by tablePath (for retriever), then by partitionName (for batch offset resolution)
        Map<TablePath, Map<String, List<TableBucketInfo>>> grouped = new LinkedHashMap<>();
        for (TableBucketInfo info : newBuckets) {
            grouped.computeIfAbsent(
                            info.physicalTablePath.getTablePath(), k -> new LinkedHashMap<>())
                    .computeIfAbsent(
                            info.physicalTablePath.getPartitionName(), k -> new ArrayList<>())
                    .add(info);
        }

        for (Map.Entry<TablePath, Map<String, List<TableBucketInfo>>> tableEntry :
                grouped.entrySet()) {
            TablePath tablePath = tableEntry.getKey();
            LOG.info("Initializing bucket splits for table: {}", tablePath);
            OffsetsInitializer.BucketOffsetsRetriever retriever =
                    new BucketOffsetsRetrieverImpl(admin, tablePath);

            // Check once per table whether this is a PK table in full (snapshot) mode
            boolean isPrimaryKeyTable =
                    tableEntry.getValue().values().stream()
                            .flatMap(List::stream)
                            .findFirst()
                            .map(info -> info.hasPrimaryKey)
                            .orElse(false);
            boolean readSnapshot =
                    isPrimaryKeyTable && offsetsInitializer instanceof SnapshotOffsetsInitializer;

            for (Map.Entry<String, List<TableBucketInfo>> partitionEntry :
                    tableEntry.getValue().entrySet()) {
                String partitionName = partitionEntry.getKey();
                List<TableBucketInfo> bucketInfos = partitionEntry.getValue();

                if (readSnapshot) {
                    newSplits.addAll(
                            initHybridSnapshotLogSplits(
                                    tablePath, partitionName, bucketInfos, retriever));
                } else {
                    newSplits.addAll(
                            initLogTableSplits(tablePath, partitionName, bucketInfos, retriever));
                }
            }
        }
        return newSplits;
    }

    /**
     * Creates splits for primary key table buckets in "full" startup mode. Retrieves KV snapshots
     * and creates {@link FlussHybridSnapshotLogSplit} for buckets with a snapshot, and falls back
     * to {@link FlussLogSplit} for buckets without a snapshot.
     */
    private List<FlussSplitBase> initHybridSnapshotLogSplits(
            TablePath tablePath,
            @Nullable String partitionName,
            List<TableBucketInfo> bucketInfos,
            OffsetsInitializer.BucketOffsetsRetriever retriever)
            throws Exception {
        List<FlussSplitBase> splits = new ArrayList<>();

        // Get KV snapshots for this table/partition
        KvSnapshots kvSnapshots =
                partitionName == null
                        ? admin.getLatestKvSnapshots(tablePath).get()
                        : admin.getLatestKvSnapshots(tablePath, partitionName).get();

        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (TableBucketInfo info : bucketInfos) {
            int bucketId = info.tableBucket.getBucket();
            OptionalLong snapshotId = kvSnapshots.getSnapshotId(bucketId);
            if (snapshotId.isPresent()) {
                OptionalLong logOffset = kvSnapshots.getLogOffset(bucketId);
                if (!logOffset.isPresent()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Missing log offset for snapshot %d of table-bucket %s.",
                                    snapshotId.getAsLong(), info.tableBucket));
                }
                splits.add(
                        new FlussHybridSnapshotLogSplit(
                                info.physicalTablePath,
                                info.tableBucket,
                                snapshotId.getAsLong(),
                                logOffset.getAsLong()));
            } else {
                bucketsNeedInitOffset.add(bucketId);
            }
        }

        // For buckets without a snapshot, fall back to log splits using SnapshotOffsetsInitializer
        // (which returns earliest offsets)
        if (!bucketsNeedInitOffset.isEmpty()) {
            Map<Integer, Long> bucketOffsets =
                    offsetsInitializer.getBucketOffsets(
                            partitionName, bucketsNeedInitOffset, retriever);
            validateBucketOffsets(tablePath, partitionName, bucketsNeedInitOffset, bucketOffsets);
            for (TableBucketInfo info : bucketInfos) {
                int bucketId = info.tableBucket.getBucket();
                if (bucketsNeedInitOffset.contains(bucketId)) {
                    splits.add(
                            new FlussLogSplit(
                                    info.physicalTablePath,
                                    info.tableBucket,
                                    bucketOffsets.get(bucketId)));
                }
            }
        }

        return splits;
    }

    /** Creates log-only splits for non-primary-key tables or non-snapshot startup modes. */
    private List<FlussSplitBase> initLogTableSplits(
            TablePath tablePath,
            @Nullable String partitionName,
            List<TableBucketInfo> bucketInfos,
            OffsetsInitializer.BucketOffsetsRetriever retriever) {
        List<FlussSplitBase> splits = new ArrayList<>();
        List<Integer> bucketIds =
                bucketInfos.stream()
                        .map(info -> info.tableBucket.getBucket())
                        .collect(Collectors.toList());

        Map<Integer, Long> bucketOffsets =
                offsetsInitializer.getBucketOffsets(partitionName, bucketIds, retriever);
        validateBucketOffsets(tablePath, partitionName, bucketIds, bucketOffsets);

        for (TableBucketInfo info : bucketInfos) {
            long offset = bucketOffsets.get(info.tableBucket.getBucket());
            splits.add(new FlussLogSplit(info.physicalTablePath, info.tableBucket, offset));
        }
        return splits;
    }

    private static void validateBucketOffsets(
            TablePath tablePath,
            @Nullable String partitionName,
            Collection<Integer> expectedBucketIds,
            @Nullable Map<Integer, Long> bucketOffsets) {
        List<Integer> missingBucketIds =
                expectedBucketIds.stream()
                        .filter(
                                bucketId ->
                                        bucketOffsets == null
                                                || bucketOffsets.get(bucketId) == null)
                        .collect(Collectors.toList());
        if (!missingBucketIds.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Offsets initializer did not return offsets for buckets %s of table %s%s.",
                            missingBucketIds,
                            tablePath,
                            partitionName == null ? "" : ", partition " + partitionName));
        }
    }

    // -------------------------------------------------------------------------
    //  Phase 4: Handle new splits — mark assigned and assign to readers (callback)
    // -------------------------------------------------------------------------

    /**
     * Receives newly created splits, records their {@link PhysicalTablePath}s as assigned, and
     * distributes the splits to registered readers.
     */
    private void handleTableBucketChanges(List<FlussSplitBase> newSplits, Throwable error) {
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to initialize splits for new table-buckets.", error);
        }

        if (newSplits.isEmpty()) {
            throw new FlinkRuntimeException("No splits were created for discovered table-buckets.");
        }

        addPartitionSplitChangeToPendingAssignments(newSplits);
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    // -------------------------------------------------------------------------
    //  Split assignment
    // -------------------------------------------------------------------------

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<FlussSplitBase> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (FlussSplitBase split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split.getTableBucket(), numReaders);
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<FlussSplitBase>> incrementalAssignment = new HashMap<>();

        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final Set<FlussSplitBase> pendingAssignmentForReader =
                    pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                incrementalAssignment
                        .computeIfAbsent(pendingReader, k -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending partitions as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            assignedPhysicalTablePaths.add(split.getPhysicalTablePath());
                        });
            }
        }

        if (!incrementalAssignment.isEmpty()) {
            int splitCount = incrementalAssignment.values().stream().mapToInt(List::size).sum();
            LOG.info(
                    "Assigning {} splits to {} readers.", splitCount, incrementalAssignment.size());
            LOG.debug("Split assignment: {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    static int getSplitOwner(TableBucket tableBucket, int numReaders) {
        return ((tableBucket.hashCode() * 31) & 0x7FFFFFFF) % numReaders;
    }

    // -------------------------------------------------------------------------
    //  SplitEnumerator lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Received split request from subtask {}", subtaskId);
    }

    @Override
    public void addSplitsBack(List<FlussSplitBase> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        addPartitionSplitChangeToPendingAssignments(splits);
        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} added, assigning pending splits.", subtaskId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public FlussSourceEnumState snapshotState(long checkpointId) throws Exception {
        List<FlussSplitBase> remainingSplits = new ArrayList<>();
        pendingPartitionSplitAssignment.forEach((reader, splits) -> remainingSplits.addAll(splits));
        return new FlussSourceEnumState(assignedPhysicalTablePaths, remainingSplits);
    }

    @Override
    public void close() throws IOException {
        try {
            if (discoverer != null) {
                discoverer.close();
            }
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close Fluss connection", e);
        }
    }

    /**
     * Converts a Fluss {@link org.apache.fluss.config.Configuration} to a flink-cdc-common {@link
     * Configuration} by copying all key-value pairs. Used as a fallback when no explicit source
     * config is provided.
     */
    static Configuration toSourceConfig(org.apache.fluss.config.Configuration flussConfig) {
        Map<String, String> map = new HashMap<>();
        // Extract bootstrap.servers from fluss config
        String bootstrapServers =
                flussConfig
                        .toMap()
                        .get(org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers != null) {
            map.put("bootstrap.servers", bootstrapServers);
        }
        // Copy all client.* properties as properties.client.*
        flussConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("client.")) {
                                map.put("properties." + key, value);
                            }
                        });
        return Configuration.fromMap(map);
    }

    /** Container for a discovered table-bucket with its {@link PhysicalTablePath}. */
    private static class TableBucketInfo {
        final PhysicalTablePath physicalTablePath;
        final TableBucket tableBucket;
        final boolean hasPrimaryKey;

        TableBucketInfo(
                PhysicalTablePath physicalTablePath,
                TableBucket tableBucket,
                boolean hasPrimaryKey) {
            this.physicalTablePath = physicalTablePath;
            this.tableBucket = tableBucket;
            this.hasPrimaryKey = hasPrimaryKey;
        }
    }
}
