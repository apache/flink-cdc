/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WatermarkKind;
import com.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import io.debezium.data.Envelope;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SIGNAL_EVENT_VALUE_SCHEMA_NAME;
import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SPLIT_ID_KEY;
import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WATERMARK_KIND;
import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

/** Utility class to deal record. */
public class RecordUtils {

    private RecordUtils() {}

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";
    public static final String SCHEMA_HEARTBEAT_EVENT_KEY_NAME =
            "io.debezium.connector.common.Heartbeat";
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    /** Converts a {@link ResultSet} row to an array of Objects. */
    public static Object[] rowToArray(ResultSet rs, int size) throws SQLException {
        final Object[] row = new Object[size];
        for (int i = 0; i < size; i++) {
            row[i] = rs.getObject(i + 1);
        }
        return row;
    }

    /** upsert binlog events to snapshot events collection. */
    public static void upsertBinlog(
            Map<Struct, SourceRecord> snapshotRecords, SourceRecord binlogRecord) {
        Struct key = (Struct) binlogRecord.key();
        Struct value = (Struct) binlogRecord.value();
        if (value != null) {
            Envelope.Operation operation =
                    Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            switch (operation) {
                case CREATE:
                case UPDATE:
                    Envelope envelope = Envelope.fromSchema(binlogRecord.valueSchema());
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                    Instant fetchTs =
                            Instant.ofEpochMilli((Long) source.get(Envelope.FieldName.TIMESTAMP));
                    SourceRecord record =
                            new SourceRecord(
                                    binlogRecord.sourcePartition(),
                                    binlogRecord.sourceOffset(),
                                    binlogRecord.topic(),
                                    binlogRecord.kafkaPartition(),
                                    binlogRecord.keySchema(),
                                    binlogRecord.key(),
                                    binlogRecord.valueSchema(),
                                    envelope.read(after, source, fetchTs));
                    snapshotRecords.put(key, record);
                    break;
                case DELETE:
                    snapshotRecords.remove(key);
                    break;
                case READ:
                    throw new IllegalStateException(
                            String.format(
                                    "Binlog record shouldn't use READ operation, the the record is %s.",
                                    binlogRecord));
            }
        }
    }

    /**
     * Format message timestamp(source.ts_ms) value to 0L for all records read in snapshot phase.
     */
    public static List<SourceRecord> formatMessageTimestamp(
            Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .map(
                        record -> {
                            Envelope envelope = Envelope.fromSchema(record.valueSchema());
                            Struct value = (Struct) record.value();
                            Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                            source.put(Envelope.FieldName.TIMESTAMP, 0L);
                            // extend the fetch timestamp(ts_ms)
                            Instant fetchTs =
                                    Instant.ofEpochMilli(
                                            value.getInt64(Envelope.FieldName.TIMESTAMP));
                            SourceRecord sourceRecord =
                                    new SourceRecord(
                                            record.sourcePartition(),
                                            record.sourceOffset(),
                                            record.topic(),
                                            record.kafkaPartition(),
                                            record.keySchema(),
                                            record.key(),
                                            record.valueSchema(),
                                            envelope.read(updateAfter, source, fetchTs));
                            return sourceRecord;
                        })
                .collect(Collectors.toList());
    }

    public static boolean isWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent();
    }

    public static boolean isLowWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.LOW) {
            return true;
        }
        return false;
    }

    public static boolean isHighWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.HIGH) {
            return true;
        }
        return false;
    }

    public static boolean isEndWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.BINLOG_END) {
            return true;
        }
        return false;
    }

    public static BinlogOffset getWatermark(SourceRecord watermarkEvent) {
        return getBinlogPosition(watermarkEvent.sourceOffset());
    }

    /**
     * Return the timestamp when the change event is produced in MySQL.
     *
     * <p>The field `source.ts_ms` in {@link SourceRecord} data struct is the time when the change
     * event is operated in MySQL.
     */
    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }

        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /**
     * Return the timestamp when the change event is fetched in {@link DebeziumReader}.
     *
     * <p>The field `ts_ms` in {@link SourceRecord} data struct is the time when the record fetched
     * by debezium reader, use it as the process time in Source.
     */
    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }
        return value.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    public static boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        if (keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name())) {
            return true;
        }
        return false;
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
    }

    /**
     * Return the finished snapshot split information.
     *
     * @return [splitId, splitStart, splitEnd, highWatermark], the information will be used to
     *     filter binlog events when read binlog of table.
     */
    public static FinishedSnapshotSplitInfo getSnapshotSplitInfo(
            MySqlSnapshotSplit split, SourceRecord highWatermark) {
        Struct value = (Struct) highWatermark.value();
        String splitId = value.getString(SPLIT_ID_KEY);
        return new FinishedSnapshotSplitInfo(
                split.getTableId(),
                splitId,
                split.getSplitStart(),
                split.getSplitEnd(),
                getBinlogPosition(highWatermark.sourceOffset()));
    }

    /** Returns the start offset of the binlog split. */
    public static BinlogOffset getStartingOffsetOfBinlogSplit(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits) {
        BinlogOffset startOffset =
                finishedSnapshotSplits.isEmpty()
                        ? BinlogOffset.ofEarliest()
                        : finishedSnapshotSplits.get(0).getHighWatermark();
        for (FinishedSnapshotSplitInfo finishedSnapshotSplit : finishedSnapshotSplits) {
            if (finishedSnapshotSplit.getHighWatermark().isBefore(startOffset)) {
                startOffset = finishedSnapshotSplit.getHighWatermark();
            }
        }
        return startOffset;
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(dbName, null, tableName);
    }

    public static Object[] getSplitKey(
            RowType splitBoundaryType, SourceRecord dataRecord, SchemaNameAdjuster nameAdjuster) {
        // the split key field contains single field now
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames().get(0));
        Struct key = (Struct) dataRecord.key();
        return new Object[] {key.get(splitFieldName)};
    }

    public static BinlogOffset getBinlogPosition(SourceRecord dataRecord) {
        return getBinlogPosition(dataRecord.sourceOffset());
    }

    public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return BinlogOffset.builder().setOffsetMap(offsetStrMap).build();
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        // for all range
        if (splitKeyStart == null && splitKeyEnd == null) {
            return true;
        }
        // first split
        if (splitKeyStart == null) {
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                    && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0);
        }
        // last split
        else if (splitKeyEnd == null) {
            int[] lowerBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
            }
            return Arrays.stream(lowerBoundRes).allMatch(value -> value >= 0);
        }
        // other split
        else {
            int[] lowerBoundRes = new int[key.length];
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(lowerBoundRes).anyMatch(value -> value >= 0)
                    && (Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                            && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0));
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
            return ((Comparable) o1).compareTo(o2);
        } else if (isNumericObject(o1) && isNumericObject(o2)) {
            return toBigDecimal(o1).compareTo(toBigDecimal(o2));
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    }

    private static boolean isNumericObject(Object obj) {
        return obj instanceof Byte
                || obj instanceof Short
                || obj instanceof Integer
                || obj instanceof Long
                || obj instanceof Float
                || obj instanceof Double
                || obj instanceof BigInteger
                || obj instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object numericObj) {
        return new BigDecimal(numericObj.toString());
    }

    public static HistoryRecord getHistoryRecord(SourceRecord schemaRecord) throws IOException {
        Struct value = (Struct) schemaRecord.value();
        String historyRecordStr = value.getString(HISTORY_RECORD_FIELD);
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    private static Optional<WatermarkKind> getWatermarkKind(SourceRecord record) {
        if (record.valueSchema() != null
                && SIGNAL_EVENT_VALUE_SCHEMA_NAME.equals(record.valueSchema().name())) {
            Struct value = (Struct) record.value();
            return Optional.of(WatermarkKind.valueOf(value.getString(WATERMARK_KIND)));
        }
        return Optional.empty();
    }
}
