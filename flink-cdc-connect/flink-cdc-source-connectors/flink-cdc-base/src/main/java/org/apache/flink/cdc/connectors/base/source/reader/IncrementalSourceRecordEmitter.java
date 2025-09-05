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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getFetchTimestamp;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getMessageTimestamp;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isHeartbeatEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
public class IncrementalSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    protected final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    protected final SourceReaderMetrics sourceReaderMetrics;
    protected final boolean includeSchemaChanges;
    protected final OutputCollector<T> outputCollector;
    protected final OffsetFactory offsetFactory;

    public IncrementalSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.outputCollector = new OutputCollector<>();
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), output, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            LOG.trace("Process WatermarkEvent: {}; splitState = {}", element, splitState);
            Offset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                LOG.trace("Set HighWatermark {} for {}", watermark, splitState);
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            LOG.trace("Process SchemaChangeEvent: {}; splitState = {}", element, splitState);
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            LOG.trace("Process DataChangeRecord: {}; splitState = {}", element, splitState);
            updateStreamSplitState(splitState, element);
            reportMetrics(element);
            emitElement(element, output);
        } else if (isHeartbeatEvent(element)) {
            LOG.trace("Process Heartbeat: {}; splitState = {}", element, splitState);
            updateStreamSplitState(splitState, element);
        } else {
            // unknown element
            LOG.info(
                    "Meet unknown element {} for splitState = {}, just skip.", element, splitState);
            sourceReaderMetrics.addNumRecordsInErrors(1L);
        }
    }

    private void updateStreamSplitState(SourceSplitState splitState, SourceRecord element) {
        if (splitState.isStreamSplitState()) {
            Offset position = getOffsetPosition(element);
            splitState.asStreamSplitState().setStartingOffset(position);
        }
    }

    private Offset getWatermark(SourceRecord watermarkEvent) {
        return getOffsetPosition(watermarkEvent.sourceOffset());
    }

    public Offset getOffsetPosition(SourceRecord dataRecord) {
        return getOffsetPosition(dataRecord.sourceOffset());
    }

    public Offset getOffsetPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return offsetFactory.newOffset(offsetStrMap);
    }

    protected void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        sourceReaderMetrics.markRecord();
        sourceReaderMetrics.updateRecordCounters(element);

        outputCollector.output = output;
        outputCollector.currentMessageTimestamp = getMessageTimestamp(element);
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    protected void reportMetrics(SourceRecord element) {
        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null) {
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
        }
    }

    /** An adapter between {@link SourceOutput} and {@link Collector}. */
    protected static class OutputCollector<T> implements Collector<T> {
        public SourceOutput<T> output;
        public Long currentMessageTimestamp;

        @Override
        public void collect(T record) {
            if (currentMessageTimestamp != null && currentMessageTimestamp > 0) {
                // Only binlog event contains a valid timestamp. We use the output with timestamp to
                // report the event time and let the source operator to report
                // "currentEmitEventTimeLag" correctly.
                output.collect(record, currentMessageTimestamp);
            } else {
                // Records in snapshot mode have a zero timestamp in the message. We use the output
                // without timestamp to collect the record. Metric "currentEmitEventTimeLag" will
                // not be updated in the source operator in this case.
                output.collect(record);
            }
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
