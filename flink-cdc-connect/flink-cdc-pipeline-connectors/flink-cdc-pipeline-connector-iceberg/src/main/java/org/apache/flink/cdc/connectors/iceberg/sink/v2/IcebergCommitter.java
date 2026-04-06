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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HadoopConfUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.runtime.checkpoint.CheckpointIDCounter.INITIAL_CHECKPOINT_ID;

/** A {@link Committer} for Apache Iceberg. */
public class IcebergCommitter implements Committer<WriteResultWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCommitter.class);

    public static final String NAMESPACE_GROUP_KEY = "namespace";

    public static final String SCHEMA_GROUP_KEY = "schema";

    public static final String TABLE_GROUP_KEY = "table";

    /** Snapshot summary key for the batch index; used to resume partial commits on retry. */
    static final String FLINK_BATCH_INDEX = "flink.batch-index";

    /** Snapshot summary key for the checkpoint ID on intermediate batch commits. */
    static final String FLINK_CHECKPOINT_ID_PROP = "flink.checkpoint-id";

    private final Catalog catalog;

    private final SinkCommitterMetricGroup metricGroup;

    private final Map<TableId, TableMetric> tableIdMetricMap;

    public IcebergCommitter(
            Map<String, String> catalogOptions, Map<String, String> hadoopConfOptions) {
        this(catalogOptions, null, hadoopConfOptions);
    }

    public IcebergCommitter(
            Map<String, String> catalogOptions,
            SinkCommitterMetricGroup metricGroup,
            Map<String, String> hadoopConfOptions) {
        Configuration configuration = HadoopConfUtils.createConfiguration(hadoopConfOptions);
        this.catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, configuration);
        this.metricGroup = metricGroup;
        this.tableIdMetricMap = new HashMap<>();
    }

    @Override
    public void commit(Collection<CommitRequest<WriteResultWrapper>> collection) {
        List<WriteResultWrapper> results =
                collection.stream().map(CommitRequest::getCommittable).collect(toList());
        commit(results);
    }

    private void commit(List<WriteResultWrapper> writeResultWrappers) {
        if (writeResultWrappers.isEmpty()) {
            return;
        }
        long checkpointId = writeResultWrappers.get(0).getCheckpointId();
        String newFlinkJobId = writeResultWrappers.get(0).getJobId();
        String operatorId = writeResultWrappers.get(0).getOperatorId();

        Map<TableId, List<WriteResultWrapper>> tableMap = new HashMap<>();
        for (WriteResultWrapper w : writeResultWrappers) {
            tableMap.computeIfAbsent(w.getTableId(), k -> new ArrayList<>()).add(w);
        }

        for (Map.Entry<TableId, List<WriteResultWrapper>> entry : tableMap.entrySet()) {
            TableId tableId = entry.getKey();

            // Group by batchIndex so wrappers from different subtasks for the same batch
            // are merged into one snapshot, not committed separately.
            TreeMap<Integer, List<WriteResultWrapper>> batchGroups = new TreeMap<>();
            for (WriteResultWrapper w : entry.getValue()) {
                batchGroups.computeIfAbsent(w.getBatchIndex(), k -> new ArrayList<>()).add(w);
                LOGGER.info(w.buildDescription());
            }

            Table table =
                    catalog.loadTable(
                            TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));

            int startBatchIndex = 0;
            Snapshot snapshot = table.currentSnapshot();
            if (snapshot != null) {
                Iterable<Snapshot> ancestors =
                        SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
                long lastCommittedCheckpointId =
                        getMaxCommittedCheckpointId(ancestors, newFlinkJobId, operatorId);
                if (lastCommittedCheckpointId >= checkpointId) {
                    LOGGER.warn(
                            "Checkpoint id {} has been committed to table {}, skipping",
                            checkpointId,
                            tableId.identifier());
                    continue;
                }
                ancestors = SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
                startBatchIndex =
                        getLastCommittedBatchIndex(
                                        ancestors, newFlinkJobId, operatorId, checkpointId)
                                + 1;
            }

            Optional<TableMetric> tableMetric = getTableMetric(tableId);
            tableMetric.ifPresent(TableMetric::increaseCommitTimes);

            int lastNonEmptyBatchIndex = -1;
            for (Map.Entry<Integer, List<WriteResultWrapper>> g : batchGroups.entrySet()) {
                List<DataFile> df = collectDataFilesFromGroup(g.getValue());
                List<DeleteFile> del = collectDeleteFilesFromGroup(g.getValue());
                if (!df.isEmpty() || !del.isEmpty()) {
                    lastNonEmptyBatchIndex = g.getKey();
                }
            }

            // Commit each batch as a separate snapshot so sequence numbers increase per batch.
            for (Map.Entry<Integer, List<WriteResultWrapper>> g : batchGroups.entrySet()) {
                int batchIdx = g.getKey();
                if (batchIdx < startBatchIndex) {
                    LOGGER.info(
                            "Batch {} for checkpoint {} of table {} already committed, skipping",
                            batchIdx,
                            checkpointId,
                            tableId.identifier());
                    continue;
                }

                List<DataFile> dataFiles = collectDataFilesFromGroup(g.getValue());
                List<DeleteFile> deleteFiles = collectDeleteFilesFromGroup(g.getValue());

                if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
                    LOGGER.info(
                            "Batch {} for checkpoint {} of table {} has nothing to commit, skipping",
                            batchIdx,
                            checkpointId,
                            tableId.identifier());
                    continue;
                }

                SnapshotUpdate<?> operation;
                if (deleteFiles.isEmpty()) {
                    AppendFiles append = table.newAppend();
                    dataFiles.forEach(append::appendFile);
                    operation = append;
                } else {
                    RowDelta delta = table.newRowDelta();
                    dataFiles.forEach(delta::addRows);
                    deleteFiles.forEach(delta::addDeletes);
                    operation = delta;
                }

                operation.set(SinkUtil.FLINK_JOB_ID, newFlinkJobId);
                operation.set(SinkUtil.OPERATOR_ID, operatorId);
                operation.set(FLINK_BATCH_INDEX, String.valueOf(batchIdx));
                operation.set(FLINK_CHECKPOINT_ID_PROP, String.valueOf(checkpointId));
                if (batchIdx == lastNonEmptyBatchIndex) {
                    operation.set(
                            SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, String.valueOf(checkpointId));
                }
                operation.commit();
            }
        }
    }

    private static List<DataFile> collectDataFilesFromGroup(List<WriteResultWrapper> group) {
        return group.stream()
                .flatMap(w -> collectDataFiles(w.getWriteResult()).stream())
                .collect(toList());
    }

    private static List<DeleteFile> collectDeleteFilesFromGroup(List<WriteResultWrapper> group) {
        return group.stream()
                .flatMap(w -> collectDeleteFiles(w.getWriteResult()).stream())
                .collect(toList());
    }

    private static List<DataFile> collectDataFiles(WriteResult result) {
        if (result.dataFiles() == null) {
            return new ArrayList<>();
        }
        return Arrays.stream(result.dataFiles()).filter(f -> f.recordCount() > 0).collect(toList());
    }

    private static List<DeleteFile> collectDeleteFiles(WriteResult result) {
        if (result.deleteFiles() == null) {
            return new ArrayList<>();
        }
        return Arrays.stream(result.deleteFiles())
                .filter(f -> f.recordCount() > 0)
                .collect(toList());
    }

    private static long getMaxCommittedCheckpointId(
            Iterable<Snapshot> ancestors, String flinkJobId, String operatorId) {
        long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID - 1;

        for (Snapshot ancestor : ancestors) {
            Map<String, String> summary = ancestor.summary();
            String snapshotFlinkJobId = summary.get(SinkUtil.FLINK_JOB_ID);
            String snapshotOperatorId = summary.get(SinkUtil.OPERATOR_ID);
            if (flinkJobId.equals(snapshotFlinkJobId)
                    && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
                String value = summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID);
                if (value != null) {
                    lastCommittedCheckpointId = Long.parseLong(value);
                    break;
                }
            }
        }

        return lastCommittedCheckpointId;
    }

    /**
     * Returns the highest batch index already committed for the given checkpoint, or -1 if none.
     * Used to skip already-persisted batches on retry.
     */
    private static int getLastCommittedBatchIndex(
            Iterable<Snapshot> ancestors, String flinkJobId, String operatorId, long checkpointId) {
        for (Snapshot ancestor : ancestors) {
            Map<String, String> summary = ancestor.summary();
            if (!flinkJobId.equals(summary.get(SinkUtil.FLINK_JOB_ID))) {
                continue;
            }
            String snapshotOperatorId = summary.get(SinkUtil.OPERATOR_ID);
            if (snapshotOperatorId != null && !snapshotOperatorId.equals(operatorId)) {
                continue;
            }
            // Stop once we pass a fully-committed earlier checkpoint; intermediate batch
            // snapshots for the current checkpoint lie between it and the current tip.
            String maxCommittedStr = summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID);
            if (maxCommittedStr != null && Long.parseLong(maxCommittedStr) < checkpointId) {
                break;
            }
            String snapshotCheckpointId = summary.get(FLINK_CHECKPOINT_ID_PROP);
            if (snapshotCheckpointId != null
                    && Long.parseLong(snapshotCheckpointId) == checkpointId) {
                String batchIndexStr = summary.get(FLINK_BATCH_INDEX);
                return batchIndexStr != null ? Integer.parseInt(batchIndexStr) : 0;
            }
        }
        return -1;
    }

    private Optional<TableMetric> getTableMetric(TableId tableId) {
        if (tableIdMetricMap.containsKey(tableId)) {
            return Optional.of(tableIdMetricMap.get(tableId));
        } else {
            if (metricGroup == null) {
                return Optional.empty();
            }
            MetricGroup tableIdMetricGroup =
                    metricGroup
                            .addGroup(
                                    NAMESPACE_GROUP_KEY,
                                    tableId.getNamespace() == null ? "" : tableId.getNamespace())
                            .addGroup(SCHEMA_GROUP_KEY, tableId.getSchemaName())
                            .addGroup(TABLE_GROUP_KEY, tableId.getTableName());
            TableMetric tableMetric = new TableMetric(tableIdMetricGroup);
            tableIdMetricMap.put(tableId, tableMetric);
            return Optional.of(tableMetric);
        }
    }

    @Override
    public void close() {}
}
