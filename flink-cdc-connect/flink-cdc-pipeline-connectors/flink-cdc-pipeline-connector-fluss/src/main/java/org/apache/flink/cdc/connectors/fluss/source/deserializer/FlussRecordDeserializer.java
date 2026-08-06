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

package org.apache.flink.cdc.connectors.fluss.source.deserializer;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.fluss.source.reader.FlussSourceRecord;
import org.apache.flink.cdc.connectors.fluss.utils.FlussConversions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A CDC-specific implementation of {@link FlussDeserializer} that converts Fluss {@link
 * ScanRecord}s into Flink CDC {@link Event}s (DataChangeEvents).
 *
 * <p>This class maps Fluss ChangeType to the appropriate CDC operation type (INSERT, UPDATE,
 * DELETE).
 */
public class FlussRecordDeserializer implements FlussDeserializer<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussRecordDeserializer.class);

    private static final long serialVersionUID = 1L;

    /** Cache of the last-seen schemaId per table (log records only). */
    private transient Map<TablePath, Integer> latestSchemaIdCache;

    /** Cache of the last-seen RowType per table, used to detect schema changes. */
    private transient Map<TablePath, RowType> latestRowTypeCache;

    /** Cache of row data generators per table. */
    private transient Map<TablePath, BinaryRecordDataGenerator> latestRecordDataGeneratorCache;

    /** Cache of field converters per table, used to avoid rebuilding nested type converters. */
    private transient Map<TablePath, FlussDeserializationConverter[]> latestFieldConverterCache;

    /** Tables restored from split state whose CreateTableEvent needs fresh table key metadata. */
    private transient Map<TablePath, RowType> restoredCreateTableRowTypeCache;

    @Override
    public List<Event> deserialize(FlussSourceRecord record, TablePath tablePath) {
        List<Event> events = new ArrayList<>();
        TableId tableId = TableId.tableId(tablePath.getDatabaseName(), tablePath.getTableName());
        RowType rowType = record.getRowType();

        boolean isSchemaChangeEvent = inferSchemaChangeEvent(events, record, tablePath, tableId);
        InternalRow row = record.getScanRecord().getRow();
        ChangeType changeType = record.getScanRecord().getChangeType();

        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
                {
                    RecordData after =
                            convertFlussRowToCdcRecord(
                                    tablePath, row, rowType, isSchemaChangeEvent);
                    events.add(DataChangeEvent.insertEvent(tableId, after));
                    break;
                }
            case UPDATE_BEFORE:
                // UPDATE_BEFORE is typically followed by UPDATE_AFTER.
                // We skip it here and handle the full update via UPDATE_AFTER.
                break;
            case UPDATE_AFTER:
                {
                    RecordData after =
                            convertFlussRowToCdcRecord(
                                    tablePath, row, rowType, isSchemaChangeEvent);
                    events.add(DataChangeEvent.replaceEvent(tableId, after));
                    break;
                }
            case DELETE:
                {
                    RecordData before =
                            convertFlussRowToCdcRecord(
                                    tablePath, row, rowType, isSchemaChangeEvent);
                    events.add(DataChangeEvent.deleteEvent(tableId, before));
                    break;
                }
            default:
                throw new IllegalArgumentException("Unsupported change type: " + changeType);
        }
        return events;
    }

    private boolean inferSchemaChangeEvent(
            List<Event> events, FlussSourceRecord record, TablePath tablePath, TableId tableId) {
        // Detect schema changes for log records (schemaId >= 0).
        // Snapshot records have schemaId = -1 and are skipped.
        boolean inferSchemaChangeEvent = false;
        int schemaId = record.getScanRecord().getSchemaId();
        RowType rowType = record.getRowType();
        org.apache.flink.cdc.common.types.RowType cdcRowType =
                (org.apache.flink.cdc.common.types.RowType) FlussConversions.toCdcType(rowType);
        if (schemaId >= 0) {
            ensureCacheInitialized();
            RowType restoredRowType = restoredCreateTableRowTypeCache.remove(tablePath);
            if (restoredRowType != null) {
                events.add(
                        new CreateTableEvent(
                                tableId,
                                buildCdcSchema(
                                        restoredRowType,
                                        record.getPrimaryKeyNames(),
                                        record.getPartitionKeyNames())));
            }

            Integer cachedSchemaId = latestSchemaIdCache.get(tablePath);
            if (cachedSchemaId == null || schemaId > cachedSchemaId) {
                if (cachedSchemaId == null) {
                    // First record for this table — emit CreateTableEvent with table keys.
                    events.add(
                            new CreateTableEvent(
                                    tableId,
                                    buildCdcSchema(
                                            rowType,
                                            record.getPrimaryKeyNames(),
                                            record.getPartitionKeyNames())));
                } else {
                    // SchemaId changed — infer and emit schema change events
                    inferSchemaChangeEvent = true;
                    RowType oldRowType = latestRowTypeCache.get(tablePath);
                    events.addAll(inferSchemaChanges(tableId, tablePath, oldRowType, rowType));
                }
                latestSchemaIdCache.put(tablePath, schemaId);
                latestRowTypeCache.put(tablePath, rowType);
                latestRecordDataGeneratorCache.put(
                        tablePath, new BinaryRecordDataGenerator(cdcRowType));
                latestFieldConverterCache.put(tablePath, createFieldConverters(rowType));
            }
        }
        return inferSchemaChangeEvent;
    }

    private RecordData convertFlussRowToCdcRecord(
            TablePath tablePath,
            InternalRow initialRow,
            RowType initialRowType,
            boolean isSchemaChangeEvent) {
        RowType latestRowType = latestRowTypeCache.get(tablePath);
        InternalRow row = initialRow;

        // A reader maybe subscribe multiple split, thus only inferred by the latest schema(also the
        // widest)
        if (isSchemaChangeEvent) {
            org.apache.fluss.metadata.Schema latestSchema =
                    org.apache.fluss.metadata.Schema.newBuilder()
                            .fromRowType(latestRowType)
                            .build();
            org.apache.fluss.metadata.Schema currentSchema =
                    org.apache.fluss.metadata.Schema.newBuilder()
                            .fromRowType(initialRowType)
                            .build();
            row = ProjectedRow.from(currentSchema, latestSchema).replaceRow(initialRow);
        }

        BinaryRecordDataGenerator generator = latestRecordDataGeneratorCache.get(tablePath);
        FlussDeserializationConverter[] fieldConverters = latestFieldConverterCache.get(tablePath);
        int fieldCount = latestRowType.getFieldCount();
        Object[] rowFields = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            Object flussField = fieldConverters[i].getFieldOrNull(row);
            rowFields[i] = fieldConverters[i].deserialize(flussField);
        }
        return generator.generate(rowFields);
    }

    // -------------------------------------------------------------------------
    //  Schema state restoration
    // -------------------------------------------------------------------------

    /**
     * Restores the internal schema caches from a recovered split. This seeds the
     * latestSchemaIdCache, latestRowTypeCache, and latestRecordDataGeneratorCache so that schema
     * changes occurring after the last checkpoint can still be detected.
     */
    @Override
    public List<Event> restoreState(TablePath tablePath, int schemaId, RowType rowType) {
        ensureCacheInitialized();
        // Multiple splits may read log with different schemaIds; only reserve the first one.
        if (!latestSchemaIdCache.containsKey(tablePath)) {
            latestSchemaIdCache.put(tablePath, schemaId);
            latestRowTypeCache.put(tablePath, rowType);
            org.apache.flink.cdc.common.types.RowType cdcRowType =
                    (org.apache.flink.cdc.common.types.RowType) FlussConversions.toCdcType(rowType);
            latestRecordDataGeneratorCache.put(
                    tablePath, new BinaryRecordDataGenerator(cdcRowType));
            latestFieldConverterCache.put(tablePath, createFieldConverters(rowType));
            restoredCreateTableRowTypeCache.put(tablePath, rowType);
        }
        return Collections.emptyList();
    }

    // -------------------------------------------------------------------------
    //  Schema change inference
    // -------------------------------------------------------------------------
    /** Builds a CDC schema from the given RowType, including key information. */
    private org.apache.flink.cdc.common.schema.Schema buildCdcSchema(
            RowType rowType, List<String> pkNames, List<String> partitionKeyNames) {
        org.apache.flink.cdc.common.schema.Schema.Builder schemaBuilder =
                org.apache.flink.cdc.common.schema.Schema.newBuilder();
        for (DataField field : rowType.getFields()) {
            schemaBuilder.physicalColumn(
                    field.getName(), FlussConversions.toCdcType(field.getType()));
        }

        if (pkNames != null && !pkNames.isEmpty()) {
            schemaBuilder.primaryKey(pkNames);
        }
        if (partitionKeyNames != null && !partitionKeyNames.isEmpty()) {
            schemaBuilder.partitionKey(partitionKeyNames);
        }
        return schemaBuilder.build();
    }

    private void ensureCacheInitialized() {
        if (latestSchemaIdCache == null) {
            latestSchemaIdCache = new HashMap<>();
            latestRowTypeCache = new HashMap<>();
            latestRecordDataGeneratorCache = new HashMap<>();
            latestFieldConverterCache = new HashMap<>();
            restoredCreateTableRowTypeCache = new HashMap<>();
        } else if (restoredCreateTableRowTypeCache == null) {
            latestFieldConverterCache = new HashMap<>();
            restoredCreateTableRowTypeCache = new HashMap<>();
        } else if (latestFieldConverterCache == null) {
            latestFieldConverterCache = new HashMap<>();
        }
    }

    /**
     * Compares old and new {@link RowType}s using stable column IDs ({@link
     * DataField#getFieldId()}) and produces schema change events.
     *
     * <p>Currently only "ADD COLUMN at last" is supported. Any other schema change (drop, rename,
     * type change, reorder, or insert-in-middle) will throw an {@link
     * UnsupportedOperationException}.
     */
    private List<SchemaChangeEvent> inferSchemaChanges(
            TableId tableId, TablePath tablePath, RowType oldRowType, RowType newRowType) {
        List<DataField> oldFields = oldRowType.getFields();
        List<DataField> newFields = newRowType.getFields();

        if (newFields.size() < oldFields.size()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported schema change for table %s: columns were dropped. "
                                    + "Only ADD COLUMN at last is supported. "
                                    + "Old field count: %d, new field count: %d.",
                            tablePath, oldFields.size(), newFields.size()));
        }

        // The first oldFields.size() fields must match exactly (same fieldId, name, and type)
        for (int i = 0; i < oldFields.size(); i++) {
            DataField oldField = oldFields.get(i);
            DataField newField = newFields.get(i);

            if (oldField.getFieldId() != newField.getFieldId()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported schema change for table %s: column at position %d "
                                        + "has different field ID (old: %d, new: %d). "
                                        + "Only ADD COLUMN at last is supported.",
                                tablePath, i, oldField.getFieldId(), newField.getFieldId()));
            }

            if (!oldField.getName().equals(newField.getName())) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported schema change for table %s: column '%s' was renamed "
                                        + "to '%s' at position %d. "
                                        + "Only ADD COLUMN at last is supported.",
                                tablePath, oldField.getName(), newField.getName(), i));
            }

            DataType oldCdcType = FlussConversions.toCdcType(oldField.getType());
            DataType newCdcType = FlussConversions.toCdcType(newField.getType());
            if (!oldCdcType.equals(newCdcType)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported schema change for table %s: column '%s' type changed "
                                        + "from %s to %s. "
                                        + "Only ADD COLUMN at last is supported.",
                                tablePath, oldField.getName(), oldCdcType, newCdcType));
            }
        }

        // Remaining fields in newFields are added columns at last
        if (newFields.size() == oldFields.size()) {
            // SchemaId changed but fields are identical — no schema change events needed
            return Collections.emptyList();
        }

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        for (int i = oldFields.size(); i < newFields.size(); i++) {
            DataField addedField = newFields.get(i);
            DataType cdcType = FlussConversions.toCdcType(addedField.getType());
            Column column =
                    Column.physicalColumn(
                            addedField.getName(),
                            cdcType,
                            addedField.getDescription().orElse(null));
            addedColumns.add(AddColumnEvent.last(column));
        }

        return Collections.singletonList(new AddColumnEvent(tableId, addedColumns));
    }

    private FlussDeserializationConverter[] createFieldConverters(RowType rowType) {
        FlussDeserializationConverter[] converters =
                new FlussDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            converters[i] = createNullableInternalConverter(rowType.getTypeAt(i), i);
        }
        return converters;
    }

    private FlussDeserializationConverter createNullableInternalConverter(
            org.apache.fluss.types.DataType flussDataType, int pos) {
        InternalRow.FieldGetter fieldGetter = InternalRow.createFieldGetter(flussDataType, pos);
        FlussDeserializationConverter converter = createNullableInternalConverter(flussDataType);
        return new FlussDeserializationConverter() {
            @Override
            public Object deserialize(Object flussField) {
                return converter.deserialize(flussField);
            }

            @Override
            public Object getFieldOrNull(InternalRow row) {
                return fieldGetter.getFieldOrNull(row);
            }
        };
    }

    private FlussDeserializationConverter createNullableInternalConverter(
            org.apache.fluss.types.DataType flussDataType) {
        FlussDeserializationConverter converter = createInternalConverter(flussDataType);
        return flussField -> flussField == null ? null : converter.deserialize(flussField);
    }

    private FlussDeserializationConverter createInternalConverter(
            org.apache.fluss.types.DataType flussDataType) {
        switch (flussDataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BINARY:
            case BYTES:
                return flussField -> flussField;
            case CHAR:
            case STRING:
                return flussField ->
                        BinaryStringData.fromBytes(((BinaryString) flussField).toBytes());
            case DECIMAL:
                return flussField -> {
                    Decimal decimal = (Decimal) flussField;
                    return DecimalData.fromBigDecimal(
                            decimal.toBigDecimal(), decimal.precision(), decimal.scale());
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return flussField -> {
                    TimestampNtz timestampNtz = (TimestampNtz) flussField;
                    return TimestampData.fromMillis(
                            timestampNtz.getMillisecond(), timestampNtz.getNanoOfMillisecond());
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return flussField -> {
                    TimestampLtz timestampLtz = (TimestampLtz) flussField;
                    return LocalZonedTimestampData.fromEpochMillis(
                            timestampLtz.getEpochMillisecond(),
                            timestampLtz.getNanoOfMillisecond());
                };
            case ARRAY:
                ArrayType arrayType = (ArrayType) flussDataType;
                InternalArray.ElementGetter elementGetter =
                        InternalArray.createElementGetter(arrayType.getElementType());
                FlussDeserializationConverter elementConverter =
                        createNullableInternalConverter(arrayType.getElementType());
                return flussField -> {
                    InternalArray flussArray = (InternalArray) flussField;
                    Object[] cdcArray = new Object[flussArray.size()];
                    for (int i = 0; i < flussArray.size(); i++) {
                        Object flussElement = elementGetter.getElementOrNull(flussArray, i);
                        cdcArray[i] = elementConverter.deserialize(flussElement);
                    }
                    return new GenericArrayData(cdcArray);
                };
            case MAP:
                MapType mapType = (MapType) flussDataType;
                InternalArray.ElementGetter keyGetter =
                        InternalArray.createElementGetter(mapType.getKeyType());
                InternalArray.ElementGetter valueGetter =
                        InternalArray.createElementGetter(mapType.getValueType());
                FlussDeserializationConverter keyConverter =
                        createNullableInternalConverter(mapType.getKeyType());
                FlussDeserializationConverter valueConverter =
                        createNullableInternalConverter(mapType.getValueType());
                return flussField -> {
                    InternalMap flussMap = (InternalMap) flussField;
                    InternalArray keyArray = flussMap.keyArray();
                    InternalArray valueArray = flussMap.valueArray();
                    Map<Object, Object> cdcMap = new LinkedHashMap<>();
                    for (int i = 0; i < flussMap.size(); i++) {
                        Object flussKey = keyGetter.getElementOrNull(keyArray, i);
                        Object flussValue = valueGetter.getElementOrNull(valueArray, i);
                        cdcMap.put(
                                keyConverter.deserialize(flussKey),
                                valueConverter.deserialize(flussValue));
                    }
                    return new GenericMapData(cdcMap);
                };
            case ROW:
                RowType rowType = (RowType) flussDataType;
                int fieldCount = rowType.getFieldCount();
                InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldCount];
                FlussDeserializationConverter[] fieldConverters =
                        new FlussDeserializationConverter[fieldCount];
                for (int i = 0; i < fieldCount; i++) {
                    fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
                    fieldConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
                }
                return flussField -> {
                    InternalRow flussRow = (InternalRow) flussField;
                    GenericRecordData cdcRow = new GenericRecordData(fieldCount);
                    for (int i = 0; i < fieldCount; i++) {
                        Object flussFieldValue = fieldGetters[i].getFieldOrNull(flussRow);
                        cdcRow.setField(i, fieldConverters[i].deserialize(flussFieldValue));
                    }
                    return cdcRow;
                };
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Fluss data type for deserialization: " + flussDataType);
        }
    }

    private interface FlussDeserializationConverter extends Serializable {
        Object deserialize(Object flussField);

        default Object getFieldOrNull(InternalRow row) {
            throw new UnsupportedOperationException(
                    "Only top-level converters support field access.");
        }
    }
}
