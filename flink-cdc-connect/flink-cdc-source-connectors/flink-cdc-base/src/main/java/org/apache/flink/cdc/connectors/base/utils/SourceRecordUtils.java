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

package org.apache.flink.cdc.connectors.base.utils;

import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.data.Envelope;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher.HISTORY_RECORD_FIELD;

/** Utility class to deal record. */
public class SourceRecordUtils {

    private SourceRecordUtils() {}

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.*.SchemaChangeKey";
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
     * The field `ts_ms` in {@link SourceRecord} data struct is the time when the record fetched by
     * debezium reader, use it as the process time in Source.
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
        return keySchema != null && (keySchema.name().matches(SCHEMA_CHANGE_EVENT_KEY_NAME));
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        Field field = source.schema().field(SCHEMA_NAME_KEY);
        String schemaName = null;
        if (field != null) {
            schemaName = source.getString(SCHEMA_NAME_KEY);
        }
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(dbName, schemaName, tableName);
    }

    public static Object[] getSplitKey(
            RowType splitBoundaryType, SourceRecord dataRecord, SchemaNameAdjuster nameAdjuster) {
        // the split key field contains single field now
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames().get(0));
        Struct key = (Struct) dataRecord.key();
        return new Object[] {key.get(splitFieldName)};
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        return compareKeyWithRange(key, splitKeyStart, splitKeyEnd) == RangePosition.WITHIN;
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

    /**
     * Sorts the given finished snapshot splits by their splitStart boundary in ascending order. The
     * first split (splitStart == null) is treated as negative infinity, and the last split
     * (splitEnd == null) is treated as positive infinity.
     */
    public static void sortFinishedSplitInfos(List<FinishedSnapshotSplitInfo> splits) {
        if (splits == null || splits.size() <= 1) {
            return;
        }

        splits.sort(
                (leftSplit, rightSplit) -> {
                    Object[] leftSplitStart = leftSplit.getSplitStart();
                    Object[] rightSplitStart = rightSplit.getSplitStart();

                    if (leftSplitStart == null && rightSplitStart == null) {
                        return 0;
                    }
                    if (leftSplitStart == null) {
                        return -1;
                    }
                    if (rightSplitStart == null) {
                        return 1;
                    }

                    return compareSplit(leftSplitStart, rightSplitStart);
                });
    }

    /**
     * Uses binary search to find the split containing the specified key in a sorted split list.
     *
     * <p>IMPORTANT: The splits list MUST be sorted by splitStart before calling this method. Use
     * sortFinishedSplitInfos() to sort the list if needed.
     *
     * <p>To leverage data locality for append-heavy workloads (e.g. auto-increment PKs), this
     * method checks the first and last splits before applying binary search to the remaining
     * subset.
     *
     * @param sortedSplits List of splits sorted by splitStart (MUST be sorted!)
     * @param key The chunk key to search for
     * @return The split containing the key, or null if not found
     */
    public static FinishedSnapshotSplitInfo findSplitByKeyBinary(
        List<FinishedSnapshotSplitInfo> sortedSplits, Object[] key) {

        if (sortedSplits == null || sortedSplits.isEmpty()) {
            return null;
        }

        int size = sortedSplits.size();

        FinishedSnapshotSplitInfo firstSplit = sortedSplits.get(0);
        RangePosition firstPosition =
            compareKeyWithRange(key, firstSplit.getSplitStart(), firstSplit.getSplitEnd());
        if (firstPosition == RangePosition.WITHIN) {
            return firstSplit;
        }
        if (firstPosition == RangePosition.BEFORE) {
            return null;
        }
        if (size == 1) {
            return null;
        }

        FinishedSnapshotSplitInfo lastSplit = sortedSplits.get(size - 1);
        RangePosition lastPosition =
            compareKeyWithRange(key, lastSplit.getSplitStart(), lastSplit.getSplitEnd());
        if (lastPosition == RangePosition.WITHIN) {
            return lastSplit;
        }
        if (lastPosition == RangePosition.AFTER) {
            return null;
        }
        if (size == 2) {
            return null;
        }

        int left = 1;
        int right = size - 2;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            FinishedSnapshotSplitInfo split = sortedSplits.get(mid);

            RangePosition position =
                compareKeyWithRange(key, split.getSplitStart(), split.getSplitEnd());

            if (position == RangePosition.WITHIN) {
                return split;
            } else if (position == RangePosition.BEFORE) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return null;
    }

    /** Describes the relative position of a key to a split range. */
    public enum RangePosition {
        BEFORE,
        WITHIN,
        AFTER
    }

    /**
     * Compares {@code key} against the half-open interval {@code [splitStart, splitEnd)} and
     * returns where the key lies relative to that interval.
     */
    private static RangePosition compareKeyWithRange(
        Object[] key, Object[] splitStart, Object[] splitEnd) {
        if (splitStart == null) {
            if (splitEnd == null) {
                return RangePosition.WITHIN; // Full range split
            }
            // key < splitEnd ?
            int cmp = compareSplit(key, splitEnd);
            return cmp < 0 ? RangePosition.WITHIN : RangePosition.AFTER;
        }

        if (splitEnd == null) {
            // key >= splitStart ?
            int cmp = compareSplit(key, splitStart);
            return cmp >= 0 ? RangePosition.WITHIN : RangePosition.BEFORE;
        }

        // Normal case: [splitStart, splitEnd)
        int cmpStart = compareSplit(key, splitStart);
        if (cmpStart < 0) {
            return RangePosition.BEFORE; // key < splitStart
        }

        int cmpEnd = compareSplit(key, splitEnd);
        if (cmpEnd >= 0) {
            return RangePosition.AFTER; // key >= splitEnd
        }

        return RangePosition.WITHIN; // splitStart <= key < splitEnd
    }

    private static int compareSplit(Object[] leftSplit, Object[] rightSplit) {
        // Ensure both splits have the same length
        if (leftSplit.length != rightSplit.length) {
            throw new IllegalArgumentException(
                String.format(
                    "Split key arrays must have the same length. Left: %d, Right: %d",
                    leftSplit.length, rightSplit.length));
        }

        int compareResult = 0;
        for (int i = 0; i < leftSplit.length; i++) {
            compareResult = compareObjects(leftSplit[i], rightSplit[i]);
            if (compareResult != 0) {
                break;
            }
        }
        return compareResult;
    }

}
