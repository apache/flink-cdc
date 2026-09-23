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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryMapData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.variant.BinaryVariant;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.paimon.sink.utils.TypeUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.blob.BlobWriteContext;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketAssignOperator;
import org.apache.flink.core.memory.MemorySegment;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;

/**
 * A helper class to deduce Schema of paimon table for {@link BucketAssignOperator}, and create
 * FieldGetter and GenericRow for {@link PaimonWriter}.
 */
public class PaimonWriterHelper {

    /** create a list of {@link RecordData.FieldGetter} for {@link PaimonWriter}. */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        return createFieldGetters(schema, zoneId, null);
    }

    /**
     * create a list of {@link RecordData.FieldGetter} for {@link PaimonWriter} with BLOB support.
     *
     * @param schema The CDC schema containing column definitions.
     * @param zoneId The timezone for timestamp conversion.
     * @param blobWriteContext Optional context for BLOB field handling.
     * @return List of FieldGetter for converting CDC data to Paimon data.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(
            Schema schema, ZoneId zoneId, @Nullable BlobWriteContext blobWriteContext) {
        List<Column> columns = schema.getColumns();
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            fieldGetters.add(
                    createFieldGetter(
                            column.getType(), i, zoneId, blobWriteContext, column.getName()));
        }
        return fieldGetters;
    }

    /**
     * Check if the columns of upstream schema is the same as the physical schema.
     *
     * <p>Note: Default value of column was ignored as it has no influence in {@link
     * #createFieldGetter(DataType, int, ZoneId)}.
     */
    public static Boolean sameColumnsIgnoreCommentAndDefaultValue(
            Schema upstreamSchema, Schema physicalSchema) {
        List<Column> upstreamColumns = upstreamSchema.getColumns();
        List<Column> physicalColumns = physicalSchema.getColumns();
        if (upstreamColumns.size() != physicalColumns.size()) {
            return false;
        }
        for (int i = 0; i < physicalColumns.size(); i++) {
            Column upstreamColumn = upstreamColumns.get(i);
            Column physicalColumn = physicalColumns.get(i);
            // Case sensitive.
            if (!upstreamColumn.getName().equals(physicalColumn.getName())
                    || !upstreamColumn.getType().equals(physicalColumn.getType())) {
                return false;
            }
        }
        return true;
    }

    private static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        return createFieldGetter(fieldType, fieldPos, zoneId, null, null);
    }

    private static RecordData.FieldGetter createFieldGetter(
            DataType fieldType,
            int fieldPos,
            ZoneId zoneId,
            @Nullable BlobWriteContext blobWriteContext,
            @Nullable String fieldName) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // Check if this field should be converted to BLOB type
                if (blobWriteContext != null
                        && fieldName != null
                        && blobWriteContext.isBlobField(fieldName)) {
                    if (blobWriteContext.isBlobDescriptorField(fieldName)) {
                        // blob-descriptor-field mode: Create BlobRef that stores descriptor info.
                        // Only descriptor (uri, offset, length) is stored inline, external data is
                        // NOT read or copied. Actual data reading happens on Paimon read side.
                        fieldGetter =
                                row -> {
                                    String path = row.getString(fieldPos).toString();
                                    return blobWriteContext.createBlobRef(path);
                                };
                    } else {
                        throw new IllegalArgumentException(
                                String.format(
                                        "VARCHAR/CHAR blob field '%s' must be configured in 'blob-descriptor-field'. "
                                                + "For VARCHAR/CHAR fields configured in 'blob-field', you must also configure "
                                                + "'blob-descriptor-field' to specify which fields store serialized BlobDescriptor bytes inline. "
                                                + "If you want to write raw blob data from string path, please use VARBINARY/BINARY type instead.",
                                        fieldName));
                    }
                } else {
                    fieldGetter =
                            row -> BinaryString.fromString(row.getString(fieldPos).toString());
                }
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                // Check if this field should be converted to BLOB type
                if (blobWriteContext != null
                        && fieldName != null
                        && blobWriteContext.isBlobField(fieldName)) {
                    fieldGetter =
                            row -> {
                                byte[] bytes = row.getBinary(fieldPos);
                                return blobWriteContext.createBlob(bytes);
                            };
                } else {
                    fieldGetter = row -> row.getBinary(fieldPos);
                }
                break;
            case DECIMAL:
                final int decimalPrecision = DataTypeChecks.getPrecision(fieldType);
                final int decimalScale = DataTypeChecks.getScale(fieldType);
                fieldGetter =
                        row -> {
                            DecimalData decimalData =
                                    row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                            return Decimal.fromBigDecimal(
                                    decimalData.toBigDecimal(), decimalPrecision, decimalScale);
                        };
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case INTEGER:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case DATE:
                fieldGetter = row -> (int) row.getDate(fieldPos).toEpochDay();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> (int) row.getTime(fieldPos).toMillisOfDay();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromSQLTimestamp(
                                        row.getTimestamp(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromInstant(
                                        row.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = new BinaryFieldDataGetter(fieldPos, DataTypeRoot.ROW, rowFieldCount);
                break;
            case ARRAY:
            case MAP:
                fieldGetter = new BinaryFieldDataGetter(fieldPos, fieldType.getTypeRoot());
                break;
            case VARIANT:
                fieldGetter =
                        row -> {
                            org.apache.flink.cdc.common.types.variant.Variant variant =
                                    row.getVariant(fieldPos);
                            Preconditions.checkArgument(
                                    variant instanceof BinaryVariant,
                                    "Unsupported variant type: %s",
                                    variant.getClass());
                            return new GenericVariant(
                                    ((BinaryVariant) variant).getValue(),
                                    ((BinaryVariant) variant).getMetadata());
                        };
                break;
            default:
                throw new IllegalArgumentException(
                        "don't support type of " + fieldType.getTypeRoot());
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /** create a {@link GenericRow} from a {@link DataChangeEvent} for {@link PaimonWriter}. */
    public static GenericRow convertEventToGenericRow(
            DataChangeEvent dataChangeEvent, List<RecordData.FieldGetter> fieldGetters) {
        GenericRow genericRow;
        RecordData recordData;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                {
                    recordData = dataChangeEvent.after();
                    genericRow = new GenericRow(RowKind.INSERT, recordData.getArity());
                    break;
                }
            case DELETE:
                {
                    recordData = dataChangeEvent.before();
                    genericRow = new GenericRow(RowKind.DELETE, recordData.getArity());
                    break;
                }
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRow.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRow;
    }

    /** create full {@link GenericRow}s from a {@link DataChangeEvent} for {@link PaimonWriter}. */
    public static List<GenericRow> convertEventToFullGenericRows(
            DataChangeEvent dataChangeEvent,
            List<RecordData.FieldGetter> fieldGetters,
            boolean hasPrimaryKey) {
        List<GenericRow> fullGenericRows = new ArrayList<>();
        switch (dataChangeEvent.op()) {
            case INSERT:
                {
                    fullGenericRows.add(
                            convertRecordDataToGenericRow(
                                    dataChangeEvent.after(), fieldGetters, RowKind.INSERT));
                    break;
                }
            case UPDATE:
                {
                    if (hasPrimaryKey) {
                        fullGenericRows.add(
                                convertRecordDataToGenericRow(
                                        dataChangeEvent.before(),
                                        fieldGetters,
                                        RowKind.UPDATE_BEFORE));
                    }
                    fullGenericRows.add(
                            convertRecordDataToGenericRow(
                                    dataChangeEvent.after(), fieldGetters, RowKind.UPDATE_AFTER));
                    break;
                }
            case REPLACE:
                {
                    fullGenericRows.add(
                            convertRecordDataToGenericRow(
                                    dataChangeEvent.after(), fieldGetters, RowKind.UPDATE_AFTER));
                    break;
                }
            case DELETE:
                {
                    if (hasPrimaryKey) {
                        fullGenericRows.add(
                                convertRecordDataToGenericRow(
                                        dataChangeEvent.before(), fieldGetters, RowKind.DELETE));
                    }
                    break;
                }
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
        return fullGenericRows;
    }

    /**
     * Deduce {@link Schema} for a {@link Table}.
     *
     * <p>Note: default value was not included in the result.
     */
    public static Schema deduceSchemaForPaimonTable(Table table) {
        RowType rowType = table.rowType();
        Schema.Builder builder = Schema.newBuilder();
        builder.setColumns(
                rowType.getFields().stream()
                        .map(
                                column -> {
                                    org.apache.flink.cdc.common.types.DataType cdcType;
                                    // Handle BLOB type: Paimon BLOB → CDC VARBINARY or STRING
                                    //
                                    // Paimon supports two blob storage modes:
                                    // 1. Raw data mode: VARBINARY/BINARY → BlobData → written to
                                    // .blob files
                                    // 2. Descriptor mode: STRING → BlobRef → only descriptor (uri,
                                    // offset, length)
                                    //    stored inline, actual data remains in external storage
                                    //
                                    // For descriptor mode fields (configured via
                                    // blob-descriptor-field option),
                                    // the upstream provides URI/path strings, so CDC type should be
                                    // STRING.
                                    // For raw data mode fields, the upstream provides raw bytes, so
                                    // CDC type
                                    // should be VARBINARY.
                                    if (column.type().getTypeRoot()
                                            == org.apache.paimon.types.DataTypeRoot.BLOB) {
                                        CoreOptions coreOptions =
                                                CoreOptions.fromMap(table.options());
                                        Set<String> blobDescriptorFields =
                                                coreOptions.blobDescriptorField();
                                        if (blobDescriptorFields.contains(column.name())) {
                                            // Descriptor mode: upstream provides URI string
                                            cdcType = DataTypes.STRING();
                                        } else {
                                            // Raw data mode: upstream provides raw bytes
                                            cdcType = DataTypes.VARBINARY(Integer.MAX_VALUE);
                                        }
                                    } else {
                                        // Use TypeUtils.toCDCDataType which handles VARIANT type
                                        // properly
                                        cdcType = TypeUtils.toCDCDataType(column.type());
                                    }
                                    return Column.physicalColumn(
                                            column.name(), cdcType, column.description());
                                })
                        .collect(Collectors.toList()));
        builder.primaryKey(table.primaryKeys());
        table.comment().ifPresent(builder::comment);
        builder.options(table.options());
        return builder.build();
    }

    private static GenericRow convertRecordDataToGenericRow(
            RecordData recordData, List<RecordData.FieldGetter> fieldGetters, RowKind rowKind) {
        GenericRow genericRow = new GenericRow(rowKind, recordData.getArity());
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRow.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRow;
    }

    /** A helper class for {@link PaimonWriter} to create FieldGetter and GenericRow. */
    public static class BinaryFieldDataGetter implements RecordData.FieldGetter {
        private final int fieldPos;
        private final DataTypeRoot dataTypeRoot;
        private final int rowFieldCount;

        BinaryFieldDataGetter(int fieldPos, DataTypeRoot dataTypeRoot) {
            this(fieldPos, dataTypeRoot, -1);
        }

        BinaryFieldDataGetter(int fieldPos, DataTypeRoot dataTypeRoot, int rowFieldCount) {
            this.fieldPos = fieldPos;
            this.dataTypeRoot = dataTypeRoot;
            this.rowFieldCount = rowFieldCount;
        }

        @Override
        public Object getFieldOrNull(RecordData row) {
            switch (dataTypeRoot) {
                case ARRAY:
                    return getArrayField(row);
                case MAP:
                    return getMapField(row);
                case ROW:
                    return getRecordField(row);
                default:
                    throw new IllegalArgumentException("Unsupported field type: " + dataTypeRoot);
            }
        }

        private Object getArrayField(RecordData row) {
            ArrayData arrayData = row.getArray(fieldPos);
            if (!(arrayData instanceof BinaryArrayData)) {
                throw new IllegalArgumentException(
                        "Expected BinaryArrayData but was " + arrayData.getClass().getSimpleName());
            }
            BinaryArrayData binaryArrayData = (BinaryArrayData) arrayData;
            return convertSegments(
                    binaryArrayData.getSegments(),
                    binaryArrayData.getOffset(),
                    binaryArrayData.getSizeInBytes(),
                    MemorySegmentUtils::readArrayData);
        }

        private Object getMapField(RecordData row) {
            MapData mapData = row.getMap(fieldPos);
            if (!(mapData instanceof BinaryMapData)) {
                throw new IllegalArgumentException(
                        "Expected BinaryMapData but was " + mapData.getClass().getSimpleName());
            }
            BinaryMapData binaryMapData = (BinaryMapData) mapData;
            return convertSegments(
                    binaryMapData.getSegments(),
                    binaryMapData.getOffset(),
                    binaryMapData.getSizeInBytes(),
                    MemorySegmentUtils::readMapData);
        }

        private Object getRecordField(RecordData row) {
            RecordData recordData = row.getRow(fieldPos, rowFieldCount);
            if (!(recordData instanceof BinaryRecordData)) {
                throw new IllegalArgumentException(
                        "Expected BinaryRecordData but was "
                                + recordData.getClass().getSimpleName());
            }
            BinaryRecordData binaryRecordData = (BinaryRecordData) recordData;
            return convertSegments(
                    binaryRecordData.getSegments(),
                    binaryRecordData.getOffset(),
                    binaryRecordData.getSizeInBytes(),
                    (segments, offset, sizeInBytes) ->
                            MemorySegmentUtils.readRowData(
                                    segments, rowFieldCount, offset, sizeInBytes));
        }

        private <T> T convertSegments(
                MemorySegment[] segments,
                int offset,
                int sizeInBytes,
                SegmentConverter<T> converter) {
            org.apache.paimon.memory.MemorySegment[] paimonMemorySegments =
                    new org.apache.paimon.memory.MemorySegment[segments.length];
            for (int i = 0; i < segments.length; i++) {
                MemorySegment currMemorySegment = segments[i];
                ByteBuffer byteBuffer = currMemorySegment.wrap(0, currMemorySegment.size());

                // Allocate a new byte array and copy the data from the ByteBuffer
                byte[] bytes = new byte[currMemorySegment.size()];
                byteBuffer.get(bytes);

                paimonMemorySegments[i] = org.apache.paimon.memory.MemorySegment.wrap(bytes);
            }
            return converter.convert(paimonMemorySegments, offset, sizeInBytes);
        }

        private interface SegmentConverter<T> {
            T convert(
                    org.apache.paimon.memory.MemorySegment[] segments, int offset, int sizeInBytes);
        }

        /**
         * Gets an instance of {@link InternalRow} from underlying {@link
         * org.apache.paimon.memory.MemorySegment}.
         */
        public InternalRow readRowData(
                org.apache.paimon.memory.MemorySegment[] segments,
                int numFields,
                int baseOffset,
                long offsetAndSize) {
            final int size = ((int) offsetAndSize);
            int offset = (int) (offsetAndSize >> 32);
            BinaryRow row = new BinaryRow(numFields);
            row.pointTo(segments, offset + baseOffset, size);
            return row;
        }
    }

    public static Identifier identifierFromTableId(TableId tableId) {
        return Identifier.fromString(tableId.identifier());
    }
}
