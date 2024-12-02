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

package org.apache.flink.cdc.connectors.base.source.metrics;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** A collection class for handling metrics in {@link SourceEnumeratorMetrics}. */
public class SourceEnumeratorMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceEnumeratorMetrics.class);
    // Constants
    public static final int UNDEFINED = 0;

    // Metric names
    public static final String IS_SNAPSHOTTING = "isSnapshotting";
    public static final String IS_STREAM_READING = "isStreamReading";
    public static final String NUM_TABLES_SNAPSHOTTED = "numTablesSnapshotted";
    public static final String NUM_TABLES_REMAINING = "numTablesRemaining";
    public static final String NUM_SNAPSHOT_SPLITS_PROCESSED = "numSnapshotSplitsProcessed";
    public static final String NUM_SNAPSHOT_SPLITS_REMAINING = "numSnapshotSplitsRemaining";
    public static final String NUM_SNAPSHOT_SPLITS_FINISHED = "numSnapshotSplitsFinished";
    public static final String SNAPSHOT_START_TIME = "snapshotStartTime";
    public static final String SNAPSHOT_END_TIME = "snapshotEndTime";
    public static final String NAMESPACE_GROUP_KEY = "namespace";
    public static final String SCHEMA_GROUP_KEY = "schema";
    public static final String TABLE_GROUP_KEY = "table";

    private final SplitEnumeratorMetricGroup metricGroup;

    private volatile int isSnapshotting = UNDEFINED;
    private volatile int isStreamReading = UNDEFINED;
    private volatile int numTablesRemaining = 0;

    // Map for managing per-table metrics by table identifier
    // Key: Identifier of the table
    // Value: TableMetrics related to the table
    private final Map<TableId, TableMetrics> tableMetricsMap = new ConcurrentHashMap<>();

    public SourceEnumeratorMetrics(SplitEnumeratorMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        metricGroup.gauge(IS_SNAPSHOTTING, () -> isSnapshotting);
        metricGroup.gauge(IS_STREAM_READING, () -> isStreamReading);
        metricGroup.gauge(NUM_TABLES_REMAINING, () -> numTablesRemaining);
    }

    public void enterSnapshotPhase() {
        this.isSnapshotting = 1;
    }

    public void exitSnapshotPhase() {
        this.isSnapshotting = 0;
    }

    public void enterStreamReading() {
        this.isStreamReading = 1;
    }

    public void exitStreamReading() {
        this.isStreamReading = 0;
    }

    public void registerMetrics(
            Gauge<Integer> numTablesSnapshotted,
            Gauge<Integer> numSnapshotSplitsProcessed,
            Gauge<Integer> numSnapshotSplitsRemaining) {
        metricGroup.gauge(NUM_TABLES_SNAPSHOTTED, numTablesSnapshotted);
        metricGroup.gauge(NUM_SNAPSHOT_SPLITS_PROCESSED, numSnapshotSplitsProcessed);
        metricGroup.gauge(NUM_SNAPSHOT_SPLITS_REMAINING, numSnapshotSplitsRemaining);
    }

    public void addNewTables(int numNewTables) {
        numTablesRemaining += numNewTables;
    }

    public void startSnapshotTables(int numSnapshottedTables) {
        numTablesRemaining -= numSnapshottedTables;
    }

    public TableMetrics getTableMetrics(TableId tableId) {
        return tableMetricsMap.computeIfAbsent(
                tableId,
                key -> new TableMetrics(key.catalog(), key.schema(), key.table(), metricGroup));
    }

    // ----------------------------------- Helper classes --------------------------------

    /**
     * Collection class for managing metrics of a table.
     *
     * <p>Metrics of table level are registered in its corresponding subgroup under the {@link
     * SplitEnumeratorMetricGroup}.
     */
    public static class TableMetrics {
        private AtomicInteger numSnapshotSplitsProcessed = new AtomicInteger(0);
        private AtomicInteger numSnapshotSplitsRemaining = new AtomicInteger(0);
        private AtomicInteger numSnapshotSplitsFinished = new AtomicInteger(0);
        private volatile long snapshotStartTime = UNDEFINED;
        private volatile long snapshotEndTime = UNDEFINED;

        private Set<Integer> remainingSplitChunkIds =
                Collections.newSetFromMap(new ConcurrentHashMap<>());
        private Set<Integer> processedSplitChunkIds =
                Collections.newSetFromMap(new ConcurrentHashMap<>());
        private Set<Integer> finishedSplitChunkIds =
                Collections.newSetFromMap(new ConcurrentHashMap<>());

        public TableMetrics(
                String databaseName, String schemaName, String tableName, MetricGroup parentGroup) {
            databaseName = processNull(databaseName);
            schemaName = processNull(schemaName);
            tableName = processNull(tableName);
            MetricGroup metricGroup =
                    parentGroup
                            .addGroup(NAMESPACE_GROUP_KEY, databaseName)
                            .addGroup(SCHEMA_GROUP_KEY, schemaName)
                            .addGroup(TABLE_GROUP_KEY, tableName);
            metricGroup.gauge(
                    NUM_SNAPSHOT_SPLITS_PROCESSED, () -> numSnapshotSplitsProcessed.intValue());
            metricGroup.gauge(
                    NUM_SNAPSHOT_SPLITS_REMAINING, () -> numSnapshotSplitsRemaining.intValue());
            metricGroup.gauge(
                    NUM_SNAPSHOT_SPLITS_FINISHED, () -> numSnapshotSplitsFinished.intValue());
            metricGroup.gauge(SNAPSHOT_START_TIME, () -> snapshotStartTime);
            metricGroup.gauge(SNAPSHOT_END_TIME, () -> snapshotEndTime);
            snapshotStartTime = System.currentTimeMillis();
        }

        private String processNull(String name) {
            if (StringUtils.isBlank(name)) {
                // If null, convert to an empty string
                return "";
            }
            return name;
        }

        public void addNewSplit(String newSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(newSplitId);
            if (!remainingSplitChunkIds.contains(chunkId)) {
                remainingSplitChunkIds.add(chunkId);
                numSnapshotSplitsRemaining.getAndAdd(1);
                LOGGER.info("add remaining split: {}", newSplitId);
            }
        }

        public void addNewSplits(List<String> newSplitIds) {
            if (newSplitIds != null) {
                for (String newSplitId : newSplitIds) {
                    addNewSplit(newSplitId);
                }
            }
        }

        public void removeRemainingSplit(String removeSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(removeSplitId);
            if (remainingSplitChunkIds.contains(chunkId)) {
                remainingSplitChunkIds.remove(chunkId);
                numSnapshotSplitsRemaining.getAndUpdate(num -> num - 1);
                LOGGER.info("remove remaining split: {}", removeSplitId);
            }
        }

        public void addProcessedSplit(String processedSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(processedSplitId);
            if (!processedSplitChunkIds.contains(chunkId)) {
                processedSplitChunkIds.add(chunkId);
                numSnapshotSplitsProcessed.getAndAdd(1);
                LOGGER.info("add processed split: {}", processedSplitId);
            }
        }

        public void removeProcessedSplit(String removeSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(removeSplitId);
            if (processedSplitChunkIds.contains(chunkId)) {
                processedSplitChunkIds.remove(chunkId);
                numSnapshotSplitsProcessed.getAndUpdate(num -> num - 1);
                LOGGER.info("remove processed split: {}", removeSplitId);
            }
        }

        public void reprocessSplit(String reprocessSplitId) {
            addNewSplit(reprocessSplitId);
            removeProcessedSplit(reprocessSplitId);
        }

        public void finishProcessSplit(String processedSplitId) {
            addProcessedSplit(processedSplitId);
            removeRemainingSplit(processedSplitId);
        }

        public void tryToMarkSnapshotEndTime() {
            if (numSnapshotSplitsRemaining.get() == 0
                    && (numSnapshotSplitsFinished.get() == numSnapshotSplitsProcessed.get())) {
                // Mark the end time of snapshot when remained splits is zero and processed splits
                // are all finished
                snapshotEndTime = System.currentTimeMillis();
            }
        }

        public void addFinishedSplits(Set<String> finishedSplitIds) {
            if (finishedSplitIds != null) {
                for (String finishedSplitId : finishedSplitIds) {
                    addFinishedSplit(finishedSplitId);
                }
            }
        }

        public void addFinishedSplit(String finishedSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(finishedSplitId);
            if (!finishedSplitChunkIds.contains(chunkId)) {
                finishedSplitChunkIds.add(chunkId);
                numSnapshotSplitsFinished.getAndAdd(1);
                tryToMarkSnapshotEndTime();
                LOGGER.info("add finished split: {}", finishedSplitId);
            }
        }

        public void removeFinishedSplit(String removeSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(removeSplitId);
            if (finishedSplitChunkIds.contains(chunkId)) {
                finishedSplitChunkIds.remove(chunkId);
                numSnapshotSplitsFinished.getAndUpdate(num -> num - 1);
                LOGGER.info("remove finished split: {}", removeSplitId);
            }
        }
    }
}
