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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher.HISTORY_RECORD_FIELD;

/** Utility class to deal record. */
public class SourceRecordUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SourceRecordUtils.class);

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
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames().get(0));

        // Try primary key struct first (for backward compatibility)
        Struct keyStruct = (Struct) dataRecord.key();
        if (keyStruct != null && keyStruct.schema().field(splitFieldName) != null) {
            return new Object[] {keyStruct.get(splitFieldName)};
        }

        // For non-primary key chunk keys, use value-based approach
        return getSplitKeyFromValue(dataRecord, splitFieldName);
    }

    /** Extract chunk key from value struct (AFTER/BEFORE) for non-primary key chunk keys. */
    private static Object[] getSplitKeyFromValue(SourceRecord dataRecord, String splitFieldName) {
        Struct value = (Struct) dataRecord.value();
        if (value == null) {
            return null; // No value struct available
        }

        String op = value.getString(Envelope.FieldName.OPERATION);
        Struct targetStruct = null;

        if (op == null) {
            // READ operation (snapshot)
            targetStruct = value.getStruct(Envelope.FieldName.AFTER);
        } else {
            switch (op) {
                case "c": // CREATE
                case "r": // READ
                    targetStruct = value.getStruct(Envelope.FieldName.AFTER);
                    break;
                case "u": // UPDATE - prefer AFTER for current state
                    targetStruct = value.getStruct(Envelope.FieldName.AFTER);
                    if (targetStruct == null
                            || targetStruct.schema().field(splitFieldName) == null) {
                        // Fallback to BEFORE if AFTER doesn't have the field
                        targetStruct = value.getStruct(Envelope.FieldName.BEFORE);
                    }
                    break;
                case "d": // DELETE - use BEFORE, but fallback if missing
                    targetStruct = value.getStruct(Envelope.FieldName.BEFORE);
                    if (targetStruct == null
                            || targetStruct.schema().field(splitFieldName) == null) {
                        // For DELETE with missing chunk key, return null to indicate "emit without
                        // filtering"
                        return null;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + op);
            }
        }

        if (targetStruct == null || targetStruct.schema().field(splitFieldName) == null) {
            // Chunk key field not found in value struct
            // This could happen with schema changes or configuration issues
            LOG.debug(
                    "Chunk key field '{}' not found in record, emitting without filtering. Table: {}, Operation: {}",
                    splitFieldName,
                    getTableId(dataRecord),
                    dataRecord.value() != null
                            ? ((Struct) dataRecord.value()).getString(Envelope.FieldName.OPERATION)
                            : "unknown");
            return null;
        }

        return new Object[] {targetStruct.get(splitFieldName)};
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        // If key is null, chunk key field was not found (e.g., DELETE with non-primary key chunk
        // key)
        // Emit the record without filtering to prevent data loss
        if (key == null) {
            LOG.debug("Chunk key is null, emitting record without filtering");
            return true;
        }

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
}
