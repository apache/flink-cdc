/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitState;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getWatermark;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isSchemaChangeEvent;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isWatermarkEvent;

/**
 * The {@link RecordEmitter} implementation for {@link MySQLSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public final class MySQLRecordEmitter<T>
        implements RecordEmitter<SourceRecord, T, MySQLSplitState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLRecordEmitter.class);
    private static final JsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new JsonTableChangeSerializer();

    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;

    public MySQLRecordEmitter(DebeziumDeserializationSchema<T> debeziumDeserializationSchema) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
    }

    @Override
    public void emitRecord(SourceRecord element, SourceOutput<T> output, MySQLSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            BinlogPosition watermark = getWatermark(element);
            if (isHighWatermarkEvent(element)) {
                splitState.setHighWatermarkState(watermark);
                splitState.setSnapshotReadFinishedState(true);
            } else {
                splitState.setLowWatermarkState(watermark);
            }
        } else if (isSchemaChangeEvent(element)) {
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.recordSchemaHistory(
                        tableChange.getId(),
                        new SchemaRecord(TABLE_CHANGE_SERIALIZER.toDocument(tableChange)));
            }
        } else if (isDataChangeRecord(element)) {
            BinlogPosition position = getBinlogPosition(element);
            splitState.setOffsetState(position);
            debeziumDeserializationSchema.deserialize(
                    element,
                    new Collector<T>() {
                        @Override
                        public void collect(T record) {
                            output.collect(record);
                        }

                        @Override
                        public void close() {}
                    });
        } else {
            // unknown element
            LOGGER.info("Meet unknown element {}, just skip.", element);
        }
    }
}
