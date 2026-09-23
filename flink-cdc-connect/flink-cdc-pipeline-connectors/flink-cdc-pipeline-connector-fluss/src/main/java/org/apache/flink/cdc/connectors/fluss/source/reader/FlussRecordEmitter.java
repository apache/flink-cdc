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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitState;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link RecordEmitter} that uses a {@link FlussDeserializer} to convert {@link
 * FlussSourceRecord}s into the output type {@code T}.
 *
 * <p>For <b>hybrid snapshot-log</b> splits, it distinguishes between the snapshot phase (where
 * {@code scanRecord.logOffset() < 0}) and the log phase (where {@code logOffset >= 0}):
 *
 * <ul>
 *   <li>Snapshot phase: updates {@link FlussHybridSnapshotLogSplitState#setRecordsToSkip} with the
 *       cumulative records count, so recovery can skip already-processed snapshot records.
 *   <li>Log phase: updates {@link FlussHybridSnapshotLogSplitState#setNextOffset} with the next log
 *       offset.
 * </ul>
 *
 * <p>For <b>log-only</b> splits, it advances the next offset only after successful emission, so
 * crash recovery re-reads the same offset and the deserializer can reconstruct state correctly.
 *
 * @param <T> The type of output records produced by this emitter.
 */
public class FlussRecordEmitter<T> implements RecordEmitter<FlussSourceRecord, T, FlussSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussRecordEmitter.class);

    private final FlussDeserializer<T> deserializer;

    /** Pending events to emit on the first record for each table after state restoration. */
    private final Map<TablePath, List<T>> pendingTableEvents = new HashMap<>();

    public FlussRecordEmitter(FlussDeserializer<T> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(
            FlussSourceRecord element, SourceOutput<T> output, FlussSplitState splitState)
            throws Exception {
        // Emit pending events for this table before processing the actual record.
        TablePath tablePath = element.getTablePath();
        List<T> pendingEvents = pendingTableEvents.remove(tablePath);
        if (pendingEvents != null) {
            for (T event : pendingEvents) {
                output.collect(event);
            }
        }

        ScanRecord scanRecord = element.getScanRecord();

        if (splitState.isHybridSnapshotLogSplitState()) {
            // Hybrid split: update recordsToSkip (snapshot phase) or nextOffset (log phase)
            FlussHybridSnapshotLogSplitState hybridState =
                    splitState.asHybridSnapshotLogSplitState();

            if (scanRecord.logOffset() >= 0) {
                // Record has a valid log offset — in the log (incremental) phase
                hybridState.setNextOffset(scanRecord.logOffset() + 1);
            } else {
                // Record from snapshot — update how many records to skip on recovery
                hybridState.setRecordsToSkip(element.getReadRecordsCount());
            }
            // Track schemaId for cache restoration on recovery
            updateSchemaTracking(splitState, element);
            emitRecords(element, output);
        } else if (splitState.isLogSplitState()) {
            boolean emitted = emitRecords(element, output);
            // Only advance the offset in state if records were successfully emitted.
            // This ensures that if a crash occurs, the source will re-read the same log offset
            // upon recovery, allowing the deserializer to correctly reconstruct the state.
            if (emitted && scanRecord.logOffset() >= 0) {
                splitState.asLogSplitState().setNextOffset(scanRecord.logOffset() + 1);
            }
            if (emitted) {
                updateSchemaTracking(splitState, element);
            }
        } else {
            LOG.warn("Unknown split state type: {}", splitState.getClass());
        }
    }

    private boolean emitRecords(FlussSourceRecord element, SourceOutput<T> output)
            throws Exception {
        List<T> records = deserializer.deserialize(element, element.getTablePath());

        boolean emitted = false;
        for (T record : records) {
            long timestamp = element.getScanRecord().timestamp();
            if (timestamp > 0) {
                output.collect(record, timestamp);
            } else {
                output.collect(record);
            }
            emitted = true;
        }
        return emitted;
    }

    private void updateSchemaTracking(FlussSplitState splitState, FlussSourceRecord element) {
        int schemaId = element.getScanRecord().getSchemaId();
        if (schemaId >= 0) {
            splitState.updateSchema(schemaId, element.getRowType());
        }
    }

    /**
     * Restores the deserializer's internal schema cache from a recovered split. Deserializers may
     * return pending events for emission on the first record.
     */
    public void applySplit(FlussSplitBase split) {
        if (split.getSchemaId() != null && split.getRowType() != null) {
            TablePath tablePath = split.getTablePath();
            List<T> pendingEvents =
                    deserializer.restoreState(tablePath, split.getSchemaId(), split.getRowType());
            if (!pendingEvents.isEmpty()) {
                pendingTableEvents.compute(
                        tablePath,
                        (path, list) -> {
                            List<T> newList = list != null ? list : new ArrayList<>();
                            newList.addAll(pendingEvents);
                            return newList;
                        });
            }
        }
    }
}
