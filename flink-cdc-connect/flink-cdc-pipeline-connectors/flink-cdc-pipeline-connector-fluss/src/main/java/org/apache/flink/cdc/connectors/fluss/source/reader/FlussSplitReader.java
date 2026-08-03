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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSnapshotSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A {@link SplitReader} implementation for Fluss. It reads change log records from Fluss log
 * scanners and wraps them as {@link FlussSourceRecord}s, which include the table context (table
 * path and row type) needed for downstream deserialization.
 *
 * <p>For each table, a single Fluss {@link LogScanner} is shared across all bucket-level splits.
 */
public class FlussSplitReader implements SplitReader<FlussSourceRecord, FlussSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSplitReader.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration BATCH_POLL_TIMEOUT = Duration.ofMillis(10000L);

    private final Configuration flussConfig;
    private final WrapperFlussMetricRegistry metricRegistry;
    private final FlussSourceReaderMetrics sourceReaderMetrics;
    private Connection connection;
    private final Map<TablePath, Table> tables;
    private final Map<TablePath, RowType> tableRowTypes;
    private final Map<TablePath, List<String>> tablePrimaryKeyNames;
    private final Map<TablePath, List<String>> tablePartitionKeyNames;
    private final Map<TableBucket, FlussSplitBase> bucketToSplit;

    // Bounded (snapshot) split reading
    private final Queue<FlussSplitBase> boundedSplits;
    @Nullable private FlussSplitBase currentBoundedSplit;
    @Nullable private BatchScanner currentBatchScanner;
    @Nullable private Integer currentBatchSchemaId;
    @Nullable private MultiTableLogScanner currentLogScanner;
    private long snapshotRecordsToSkip;
    private long currentReadRecordsCount;

    public FlussSplitReader(
            Configuration flussConfig,
            WrapperFlussMetricRegistry metricRegistry,
            FlussSourceReaderMetrics sourceReaderMetrics) {
        this.flussConfig = flussConfig;
        this.metricRegistry = metricRegistry;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.tables = new HashMap<>();
        this.tableRowTypes = new HashMap<>();
        this.tablePrimaryKeyNames = new HashMap<>();
        this.tablePartitionKeyNames = new HashMap<>();
        this.bucketToSplit = new HashMap<>();
        this.boundedSplits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<FlussSourceRecord> fetch() throws IOException {
        RecordsBySplits.Builder<FlussSourceRecord> builder = new RecordsBySplits.Builder<>();

        // Priority: read bounded (snapshot) splits first, then log
        checkSnapshotSplitOrStartNext();
        if (currentBatchScanner != null) {
            fetchSnapshotRecords(builder);
            return builder.build();
        }

        // Read from log scanners
        long fetchTimestamp = System.currentTimeMillis();
        long maxRecordTimestamp = -1;

        MultiTableLogScanner scanner = getOrCreateTableLogScanner();
        MultiTableRecords scanRecords = scanner.poll(POLL_TIMEOUT);
        if (scanRecords != null && !scanRecords.isEmpty()) {
            for (TablePath tablePath : scanRecords.tablePaths()) {
                for (TableBucket bucket : scanRecords.buckets(tablePath)) {
                    for (MultiTableRecord record : scanRecords.records(tablePath, bucket)) {
                        FlussSplitBase split = bucketToSplit.get(bucket);
                        if (split == null) {
                            LOG.warn("Received records for unknown bucket {}, skipping", bucket);
                            continue;
                        }
                        builder.add(
                                split.splitId(),
                                new FlussSourceRecord(
                                        record, getPartitionKeyNames(record.getTablePath())));

                        // Track offset and timestamp for metrics
                        long offset = record.logOffset();
                        if (offset >= 0) {
                            sourceReaderMetrics.recordCurrentOffset(bucket, offset);
                        }
                        maxRecordTimestamp = Math.max(maxRecordTimestamp, record.timestamp());
                    }
                }
            }
        }

        // Report event time lag
        if (maxRecordTimestamp > 0) {
            sourceReaderMetrics.reportRecordEventTime(fetchTimestamp - maxRecordTimestamp);
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FlussSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (connection == null) {
            connection = ConnectionFactory.createConnection(flussConfig, metricRegistry);
        }

        for (FlussSplitBase split : splitsChanges.splits()) {
            if (!split.isHybridSnapshotLogSplit() && !split.isLogSplit()) {
                LOG.warn("Unsupported split type: {}, skipping", split.getClass().getSimpleName());
                continue;
            }
            Table table = getOrCreateTable(split.getTablePath());
            validateTableId(split, table.getTableInfo().getTableId());
            if (split.isHybridSnapshotLogSplit()) {
                FlussHybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
                // If snapshot is not finished, add to pending bounded splits
                if (!hybrid.isSnapshotFinished()) {
                    boundedSplits.add(split);
                }
                // Still need to subscribe log for after snapshot reading
                subscribeLog(split, hybrid.getLogStartingOffset());
            } else {
                subscribeLog(split, split.asLogSplit().getStartingOffset());
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Bounded (snapshot) split reading
    // -------------------------------------------------------------------------

    /** If no bounded split is being read, poll the next one from the queue and start reading. */
    private void checkSnapshotSplitOrStartNext() {
        if (currentBatchScanner != null) {
            return;
        }

        FlussSplitBase nextSplit = boundedSplits.poll();
        if (nextSplit == null) {
            return;
        }

        currentBoundedSplit = nextSplit;
        FlussSnapshotSplit snapshotSplit = nextSplit.asSnapshotSplit();
        Table table = getOrCreateTable(nextSplit.getTablePath());
        currentBatchSchemaId = table.getTableInfo().getSchemaId();
        currentBatchScanner =
                table.newScan()
                        .createBatchScanner(
                                snapshotSplit.getTableBucket(), snapshotSplit.getSnapshotId());
        snapshotRecordsToSkip = snapshotSplit.getRecordsToSkip();
        currentReadRecordsCount = 0;
        LOG.info("Started reading snapshot for split {}", nextSplit.splitId());
    }

    /**
     * Reads a batch of snapshot records. On recovery, skips records that have already been
     * processed. Each emitted record carries its cumulative {@code readRecordsCount}.
     */
    private void fetchSnapshotRecords(RecordsBySplits.Builder<FlussSourceRecord> builder)
            throws IOException {
        assert currentBoundedSplit != null;
        assert currentBatchSchemaId != null;
        assert currentBatchScanner != null;
        TablePath tablePath = currentBoundedSplit.getTablePath();
        RowType rowType = getRowType(tablePath);

        CloseableIterator<InternalRow> batch = currentBatchScanner.pollBatch(BATCH_POLL_TIMEOUT);
        if (batch == null) {
            // Snapshot fully read
            finishCurrentBoundedSplit(builder);
            return;
        }

        try {
            while (batch.hasNext()) {
                InternalRow row = batch.next();
                currentReadRecordsCount++;
                if (snapshotRecordsToSkip > 0) {
                    snapshotRecordsToSkip--;
                    continue;
                }
                ScanRecord scanRecord =
                        new ScanRecord(
                                currentBoundedSplit.getTableBucket().getTableId(),
                                currentBatchSchemaId,
                                -1L,
                                -1L,
                                ChangeType.INSERT,
                                row,
                                // TODO: Calculate the actual record size in bytes.
                                1);
                builder.add(
                        currentBoundedSplit.splitId(),
                        new FlussSourceRecord(
                                scanRecord,
                                tablePath,
                                rowType,
                                currentReadRecordsCount,
                                getPrimaryKeyNames(tablePath),
                                getPartitionKeyNames(tablePath)));
            }
        } finally {
            batch.close();
        }
    }

    /**
     * Called when the current bounded split's snapshot is fully read. For hybrid splits, the split
     * is NOT marked as finished since log reading continues. For pure snapshot splits, the split is
     * marked as finished.
     */
    private void finishCurrentBoundedSplit(RecordsBySplits.Builder<FlussSourceRecord> builder)
            throws IOException {
        if (currentBoundedSplit.isHybridSnapshotLogSplit()) {
            // Hybrid split: snapshot done, log reading continues — do NOT finish the split
            LOG.info("Snapshot phase finished for hybrid split {}", currentBoundedSplit.splitId());
        } else {
            // Pure snapshot split: mark as finished
            builder.addFinishedSplit(currentBoundedSplit.splitId());
            LOG.info("Snapshot split {} finished", currentBoundedSplit.splitId());
        }
        closeCurrentBoundedSplit();
    }

    private void closeCurrentBoundedSplit() throws IOException {
        try {
            if (currentBatchScanner != null) {
                currentBatchScanner.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close batch scanner", e);
        }

        // todo: 可以封装为一个对象
        currentBatchScanner = null;
        currentBoundedSplit = null;
        currentBatchSchemaId = null;
    }

    // -------------------------------------------------------------------------
    //  Log subscription
    // -------------------------------------------------------------------------

    private void subscribeLog(FlussSplitBase split, long startingOffset) {
        TablePath tablePath = split.getTablePath();
        TableBucket tableBucket = split.getTableBucket();

        getOrCreateTable(tablePath);

        // Register metrics for this bucket
        sourceReaderMetrics.registerTableBucket(tableBucket);

        MultiTableLogScanner scanner = getOrCreateTableLogScanner();

        if (tableBucket.getPartitionId() != null) {
            scanner.subscribe(
                    tablePath,
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    startingOffset);
        } else {
            scanner.subscribe(tablePath, tableBucket.getBucket(), startingOffset);
        }

        bucketToSplit.put(tableBucket, split);
        LOG.info(
                "Subscribed bucket {} of table {} at offset {}",
                split.getTableBucket(),
                split.getPhysicalTablePath(),
                startingOffset);
    }

    private MultiTableLogScanner getOrCreateTableLogScanner() {
        if (currentLogScanner != null) {
            return currentLogScanner;
        }
        if (connection == null) {
            connection = ConnectionFactory.createConnection(flussConfig, metricRegistry);
        }

        currentLogScanner = connection.getMultiTable().newMultiTableScan().createLogScanner();
        return currentLogScanner;
    }

    static void validateTableId(FlussSplitBase split, long tableId) {
        long splitTableId = split.getTableBucket().getTableId();
        if (splitTableId != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table ID mismatch for split %s: split table ID is %d, but table %s has ID %d.",
                            split.splitId(), splitTableId, split.getTablePath(), tableId));
        }
    }

    protected Table getOrCreateTable(TablePath tablePath) {
        if (!tables.containsKey(tablePath)) {
            if (connection == null) {
                connection = ConnectionFactory.createConnection(flussConfig);
            }
            Table table = connection.getTable(tablePath);
            tables.put(tablePath, table);
            TableInfo tableInfo = table.getTableInfo();
            org.apache.fluss.metadata.Schema schema = tableInfo.getSchema();
            RowType rowType = schemaToRowType(schema);
            tableRowTypes.put(tablePath, rowType);
            tablePrimaryKeyNames.put(tablePath, schema.getPrimaryKeyColumnNames());
            tablePartitionKeyNames.put(tablePath, tableInfo.getPartitionKeys());
        }
        return tables.get(tablePath);
    }

    protected RowType getRowType(TablePath tablePath) {
        return tableRowTypes.get(tablePath);
    }

    protected List<String> getPrimaryKeyNames(TablePath tablePath) {
        List<String> keys = tablePrimaryKeyNames.get(tablePath);
        return keys != null ? keys : Collections.emptyList();
    }

    protected List<String> getPartitionKeyNames(TablePath tablePath) {
        List<String> keys = tablePartitionKeyNames.get(tablePath);
        return keys != null ? keys : Collections.emptyList();
    }

    private static RowType schemaToRowType(org.apache.fluss.metadata.Schema schema) {
        List<DataField> fields = new ArrayList<>();
        for (org.apache.fluss.metadata.Schema.Column column : schema.getColumns()) {
            fields.add(new DataField(column.getName(), column.getDataType(), column.getColumnId()));
        }
        return new RowType(fields);
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentBatchScanner != null) {
            try {
                currentBatchScanner.close();
            } catch (Exception e) {
                LOG.warn("Error closing batch scanner", e);
            }
        }

        if (currentLogScanner != null) {
            currentLogScanner.close();
        }
        for (Table table : tables.values()) {
            try {
                table.close();
            } catch (Exception e) {
                LOG.warn("Error closing table", e);
            }
        }
        if (connection != null) {
            connection.close();
        }
    }
}
