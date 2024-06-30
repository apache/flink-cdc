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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import io.debezium.relational.TableId;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogNewAddedTableEvent;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * The {@link RecordEmitter} implementation for {@link MySqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public class MySqlRecordEmitter<T> implements RecordEmitter<SourceRecords, T, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    private final MySqlSourceReaderContext context;
    private final MySqlSourceReaderMetrics sourceReaderMetrics;
    private final boolean includeSchemaChanges;
    private final OutputCollector<T> outputCollector;

    public MySqlRecordEmitter(
            MySqlSourceReaderContext context,
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges) {
        this.context = context;
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), output, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        if (RecordUtils.isWatermarkEvent(element)) {
            BinlogOffset watermark = RecordUtils.getWatermark(element);
            if (RecordUtils.isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (RecordUtils.isSchemaChangeEvent(element) && splitState.isBinlogSplitState()) {
            HistoryRecord historyRecord = RecordUtils.getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asBinlogSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                BinlogOffset position = RecordUtils.getBinlogPosition(element);
                splitState.asBinlogSplitState().setStartingOffset(position);
                TableId tableId = extractTableId(element);
                LOG.info("sending table add to enumerator from subtask");
                context.getSourceReaderContext().sendSourceEventToCoordinator(new BinlogNewAddedTableEvent(tableId.catalog(), tableId.schema(), tableId.table()));
//                emitElement(element, output);
            }
        } else if (RecordUtils.isDataChangeRecord(element)) {
            updateStartingOffsetForSplit(splitState, element);
            reportMetrics(element);
            emitElement(element, output);
        } else if (RecordUtils.isHeartbeatEvent(element)) {
            updateStartingOffsetForSplit(splitState, element);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    public static TableId extractTableId(SourceRecord sourceRecord) {
        // 获取 SourceRecord 的 sourcePartition
        Map<String, ?> sourcePartition = sourceRecord.sourcePartition();

        // 获取 SourceRecord 的 key，通常是 Struct 类型
        Struct value = ((Struct) sourceRecord.value()).getStruct("source");

        // 获取 sourceOffset
        Map<String, ?> sourceOffset = sourceRecord.sourceOffset();

        // 从 keyStruct 中提取 TableId
        String databaseName = value.getString("db");
//        String schemaName = value.getString("schema");
        String tableName = value.getString("table");

        // 创建 TableId 对象
        TableId tableId = new TableId(null, databaseName, tableName);

        return tableId;
    }

    private void updateStartingOffsetForSplit(MySqlSplitState splitState, SourceRecord element) {
        if (splitState.isBinlogSplitState()) {
            BinlogOffset position = RecordUtils.getBinlogPosition(element);
            splitState.asBinlogSplitState().setStartingOffset(position);
        }
    }

    private void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        outputCollector.output = output;
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    private void reportMetrics(SourceRecord element) {

        Long messageTimestamp = RecordUtils.getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = RecordUtils.getFetchTimestamp(element);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                // report fetch delay
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
        }
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
