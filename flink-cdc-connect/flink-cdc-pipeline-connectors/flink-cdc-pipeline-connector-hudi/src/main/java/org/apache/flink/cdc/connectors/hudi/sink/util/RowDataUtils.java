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

package org.apache.flink.cdc.connectors.hudi.sink.util;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.RecordData.FieldGetter;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bulk.RowDataKeyGen;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;

/** Utils for converting {@link RowData} and {@link DataChangeEvent}. */
public class RowDataUtils {

    /** Convert {@link DataChangeEvent} to {@link RowData}. */
    public static RowData convertDataChangeEventToRowData(
            ChangeEvent changeEvent, List<FieldGetter> fieldGetters) {

        if (!(changeEvent instanceof DataChangeEvent)) {
            throw new IllegalArgumentException("ChangeEvent must be of type DataChangeEvent");
        }

        DataChangeEvent dataChangeEvent = (DataChangeEvent) changeEvent;

        RecordData recordData;
        RowKind kind;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                {
                    recordData = dataChangeEvent.after();
                    kind = RowKind.INSERT;
                    break;
                }
            case DELETE:
                {
                    recordData = dataChangeEvent.before();
                    kind = RowKind.DELETE;
                    break;
                }
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
        GenericRowData genericRowData = new GenericRowData(recordData.getArity());
        genericRowData.setRowKind(kind);
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRowData;
    }

    public static List<FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<Column> columns = schema.getColumns();
        List<FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(createFieldGetter(columns.get(i).getType(), i, zoneId));
        }
        return fieldGetters;
    }

    /** Create a {@link FieldGetter} for the given {@link DataType}. */
    public static FieldGetter createFieldGetter(DataType fieldType, int fieldPos, ZoneId zoneId) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter =
                        row ->
                                org.apache.flink.table.data.StringData.fromString(
                                        row.getString(fieldPos).toString());
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalScale = DataTypeChecks.getScale(fieldType);
                int precision = getPrecision(fieldType);
                fieldGetter =
                        row -> {
                            DecimalData decimalData =
                                    row.getDecimal(fieldPos, precision, decimalScale);
                            return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                    decimalData.toBigDecimal(), precision, decimalScale);
                        };
                break;
            case TINYINT:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getInt(fieldPos);
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
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = (row) -> row.getInt(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        (row) ->
                                TimestampData.fromTimestamp(
                                        row.getTimestamp(fieldPos, getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        row ->
                                TimestampData.fromInstant(
                                        row.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        (row) ->
                                TimestampData.fromTimestamp(
                                        row.getZonedTimestamp(fieldPos, getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
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

    /**
     * Convert a DataChangeEvent to a HoodieFlinkInternalRow with automatic record key and partition
     * path extraction using Hudi's RowDataKeyGen. This is the preferred method as it uses Hudi's
     * built-in key generation logic.
     *
     * @param dataChangeEvent The DataChangeEvent to convert
     * @param schema Schema for the table
     * @param zoneId Time zone for timestamp conversion
     * @param keyGen Hudi's RowDataKeyGen for extracting record keys and partition paths
     * @param fileId The file ID for the record
     * @param instantTime The instant time for the record
     * @return HoodieFlinkInternalRow containing the converted data
     */
    public static HoodieFlinkInternalRow convertDataChangeEventToHoodieFlinkInternalRow(
            DataChangeEvent dataChangeEvent,
            Schema schema,
            ZoneId zoneId,
            org.apache.hudi.sink.bulk.RowDataKeyGen keyGen,
            String fileId,
            String instantTime) {

        // Convert DataChangeEvent to RowData using existing utility
        List<FieldGetter> fieldGetters = createFieldGetters(schema, zoneId);
        RowData rowData = convertDataChangeEventToRowData(dataChangeEvent, fieldGetters);

        // Use Hudi's RowDataKeyGen to extract record key and partition path
        String recordKey = keyGen.getRecordKey(rowData);
        String partitionPath = keyGen.getPartitionPath(rowData);

        // Map CDC operation to Hudi operation type
        String operationType = mapCdcOperationToHudiOperation(dataChangeEvent.op());

        // Create and return HoodieFlinkInternalRow
        return new HoodieFlinkInternalRow(
                recordKey, // Record key
                partitionPath, // Partition path
                fileId, // File ID
                instantTime, // Instant time
                operationType, // Operation type
                false, // isIndexRecord
                rowData // Row data
                );
    }

    /** Map CDC operation type to Hudi operation type string. */
    private static String mapCdcOperationToHudiOperation(OperationType cdcOp) {
        switch (cdcOp) {
            case INSERT:
                return "I";
            case UPDATE:
            case REPLACE:
                return "U";
            case DELETE:
                return "D";
            default:
                throw new IllegalArgumentException("Unsupported CDC operation: " + cdcOp);
        }
    }

    /**
     * Extract record key from DataChangeEvent based on primary key fields in schema. Public utility
     * method for use by operators that need to calculate record keys.
     *
     * @param dataChangeEvent The DataChangeEvent to extract record key from
     * @param schema The table schema containing primary key definitions
     * @return The record key string in format "field1:value1,field2:value2"
     */
    public static String extractRecordKeyFromDataChangeEvent(
            DataChangeEvent dataChangeEvent, Schema schema) {
        List<String> primaryKeyFields = schema.primaryKeys();
        if (primaryKeyFields.isEmpty()) {
            throw new IllegalStateException(
                    "Table " + dataChangeEvent.tableId() + " has no primary keys");
        }

        // Get the record data to extract from (after for INSERT/UPDATE/REPLACE, before for DELETE)
        RecordData recordData;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                recordData = dataChangeEvent.after();
                break;
            case DELETE:
                recordData = dataChangeEvent.before();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported operation: " + dataChangeEvent.op());
        }

        if (recordData == null) {
            throw new IllegalStateException(
                    "Record data is null for operation: " + dataChangeEvent.op());
        }

        List<String> recordKeyPairs = new ArrayList<>(primaryKeyFields.size());
        for (String primaryKeyField : primaryKeyFields) {
            int fieldIndex = schema.getColumnNames().indexOf(primaryKeyField);
            if (fieldIndex == -1) {
                throw new IllegalStateException(
                        "Primary key field '"
                                + primaryKeyField
                                + "' not found in schema for table "
                                + dataChangeEvent.tableId());
            }

            Object fieldValue =
                    recordData.isNullAt(fieldIndex)
                            ? null
                            : getFieldValue(
                                    recordData,
                                    fieldIndex,
                                    schema.getColumns().get(fieldIndex).getType());

            if (fieldValue == null) {
                throw new IllegalStateException(
                        "Primary key field '" + primaryKeyField + "' is null in record");
            }

            // Format as "fieldName:value" to match BucketAssignOperator format
            recordKeyPairs.add(primaryKeyField + ":" + fieldValue);
        }

        return String.join(",", recordKeyPairs);
    }

    /**
     * Extract partition path from DataChangeEvent based on partition key fields in schema. Public
     * utility method for use by operators that need to calculate partition paths.
     *
     * <p>If the schema has partition keys defined:
     *
     * <ul>
     *   <li>Extracts partition field values from the record data
     *   <li>Formats them as "field1=value1/field2=value2" (Hive-style partitioning)
     * </ul>
     *
     * <p>If no partition keys are defined, returns empty string (for unpartitioned tables).
     *
     * @param dataChangeEvent The DataChangeEvent to extract partition from
     * @param schema The table schema containing partition key definitions
     * @return The partition path string (empty string for unpartitioned tables)
     */
    public static String extractPartitionPathFromDataChangeEvent(
            DataChangeEvent dataChangeEvent, Schema schema) {
        List<String> partitionKeys = schema.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            // Hudi convention: unpartitioned tables use empty string, not "default"
            return "";
        }

        // Get the record data to extract from (after for INSERT/UPDATE/REPLACE, before for DELETE)
        RecordData recordData;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                recordData = dataChangeEvent.after();
                break;
            case DELETE:
                recordData = dataChangeEvent.before();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported operation: " + dataChangeEvent.op());
        }

        if (recordData == null) {
            throw new IllegalStateException(
                    "Cannot extract partition path: "
                            + dataChangeEvent.op()
                            + " event has null data");
        }

        // Extract partition values and build partition path
        List<String> partitionParts = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            int fieldIndex = schema.getColumnNames().indexOf(partitionKey);
            if (fieldIndex == -1) {
                throw new IllegalStateException(
                        "Partition key field '"
                                + partitionKey
                                + "' not found in schema for table "
                                + dataChangeEvent.tableId());
            }

            // Get field value
            Object fieldValue;
            if (recordData.isNullAt(fieldIndex)) {
                // Handle null partition values - use "__HIVE_DEFAULT_PARTITION__" as per Hive
                // convention
                fieldValue = "__HIVE_DEFAULT_PARTITION__";
            } else {
                // Get the field value based on the field type
                DataType fieldType = schema.getColumns().get(fieldIndex).getType();
                fieldValue = getFieldValue(recordData, fieldIndex, fieldType);
            }

            // Format as "key=value" (Hive-style partitioning)
            partitionParts.add(partitionKey + "=" + fieldValue);
        }

        // Join partition parts with "/"
        return String.join("/", partitionParts);
    }

    /** Get field value from RecordData based on field type. */
    private static Object getFieldValue(RecordData recordData, int fieldIndex, DataType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return recordData.getString(fieldIndex);
            case BOOLEAN:
                return recordData.getBoolean(fieldIndex);
            case BINARY:
            case VARBINARY:
                return recordData.getBinary(fieldIndex);
            case DECIMAL:
                return recordData.getDecimal(
                        fieldIndex,
                        DataTypeChecks.getPrecision(fieldType),
                        DataTypeChecks.getScale(fieldType));
            case TINYINT:
                return recordData.getByte(fieldIndex);
            case SMALLINT:
                return recordData.getShort(fieldIndex);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return recordData.getInt(fieldIndex);
            case BIGINT:
                return recordData.getLong(fieldIndex);
            case FLOAT:
                return recordData.getFloat(fieldIndex);
            case DOUBLE:
                return recordData.getDouble(fieldIndex);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return recordData.getTimestamp(fieldIndex, DataTypeChecks.getPrecision(fieldType));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return recordData.getLocalZonedTimestampData(
                        fieldIndex, DataTypeChecks.getPrecision(fieldType));
            case TIMESTAMP_WITH_TIME_ZONE:
                return recordData.getZonedTimestamp(
                        fieldIndex, DataTypeChecks.getPrecision(fieldType));
            case ROW:
                return recordData.getRow(fieldIndex, DataTypeChecks.getFieldCount(fieldType));
            default:
                throw new IllegalArgumentException(
                        "Unsupported field type: " + fieldType.getTypeRoot());
        }
    }

    /**
     * Converts a Flink CDC Schema to a Flink Table RowType.
     *
     * @param schema The input org.apache.flink.cdc.common.schema.Schema
     * @return The corresponding org.apache.flink.table.types.logical.RowType
     */
    public static RowType toRowType(Schema schema) {
        List<RowType.RowField> fields =
                schema.getColumns().stream()
                        .map(
                                column ->
                                        new RowType.RowField(
                                                column.getName(), toLogicalType(column.getType())))
                        .collect(Collectors.toList());

        return new RowType(false, fields);
    }

    /**
     * Maps a Flink CDC DataType to a Flink Table LogicalType. This method covers a wide range of
     * common types.
     *
     * @param cdcType The CDC data type
     * @return The corresponding LogicalType
     */
    public static LogicalType toLogicalType(DataType cdcType) {
        // The isNullable property is carried over.
        boolean isNullable = cdcType.isNullable();

        switch (cdcType.getTypeRoot()) {
            case CHAR:
                return new CharType(
                        isNullable,
                        ((org.apache.flink.cdc.common.types.CharType) cdcType).getLength());
            case VARCHAR:
                // STRING() in CDC is a VARCHAR with max length.
                return new VarCharType(
                        isNullable,
                        ((org.apache.flink.cdc.common.types.VarCharType) cdcType).getLength());
            case BOOLEAN:
                return new BooleanType(isNullable);
            case BINARY:
                return new BinaryType(
                        isNullable,
                        ((org.apache.flink.cdc.common.types.BinaryType) cdcType).getLength());
            case VARBINARY:
                // BYTES() in CDC is a VARBINARY with max length.
                return new VarBinaryType(
                        isNullable,
                        ((org.apache.flink.cdc.common.types.VarBinaryType) cdcType).getLength());
            case DECIMAL:
                org.apache.flink.cdc.common.types.DecimalType decimalType =
                        (org.apache.flink.cdc.common.types.DecimalType) cdcType;
                return new org.apache.flink.table.types.logical.DecimalType(
                        isNullable, decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return new TinyIntType(isNullable);
            case SMALLINT:
                return new SmallIntType(isNullable);
            case INTEGER:
                return new IntType(isNullable);
            case BIGINT:
                return new BigIntType(isNullable);
            case FLOAT:
                return new FloatType(isNullable);
            case DOUBLE:
                return new DoubleType(isNullable);
            case DATE:
                return new DateType(isNullable);
            case TIME_WITHOUT_TIME_ZONE:
                org.apache.flink.cdc.common.types.TimeType timeType =
                        (org.apache.flink.cdc.common.types.TimeType) cdcType;
                return new TimeType(isNullable, timeType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                org.apache.flink.cdc.common.types.TimestampType timestampType =
                        (org.apache.flink.cdc.common.types.TimestampType) cdcType;
                return new TimestampType(isNullable, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                org.apache.flink.cdc.common.types.LocalZonedTimestampType ltzTimestampType =
                        (org.apache.flink.cdc.common.types.LocalZonedTimestampType) cdcType;
                return new LocalZonedTimestampType(isNullable, ltzTimestampType.getPrecision());
            case ARRAY:
                org.apache.flink.cdc.common.types.ArrayType arrayType =
                        (org.apache.flink.cdc.common.types.ArrayType) cdcType;
                return new org.apache.flink.table.types.logical.ArrayType(
                        isNullable, toLogicalType(arrayType.getElementType()));
            case MAP:
                org.apache.flink.cdc.common.types.MapType mapType =
                        (org.apache.flink.cdc.common.types.MapType) cdcType;
                return new org.apache.flink.table.types.logical.MapType(
                        isNullable,
                        toLogicalType(mapType.getKeyType()),
                        toLogicalType(mapType.getValueType()));
            case ROW:
                org.apache.flink.cdc.common.types.RowType cdcRowType =
                        (org.apache.flink.cdc.common.types.RowType) cdcType;
                List<RowType.RowField> fields =
                        cdcRowType.getFields().stream()
                                .map(
                                        field ->
                                                new RowType.RowField(
                                                        field.getName(),
                                                        toLogicalType(field.getType()),
                                                        field.getDescription()))
                                .collect(Collectors.toList());
                return new org.apache.flink.table.types.logical.RowType(isNullable, fields);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported CDC type: " + cdcType.getTypeRoot());
        }
    }

    /**
     * Create a RowDataKeyGen for a table based on its schema.
     *
     * @param schema The table schema
     * @return RowDataKeyGen configured with record key and partition fields from schema
     */
    public static RowDataKeyGen createKeyGen(Schema schema) {
        Configuration config = new Configuration();

        // Set record key fields from primary keys
        List<String> primaryKeys = schema.primaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Table schema has no primary keys - cannot create RowDataKeyGen");
        }
        config.setString(FlinkOptions.RECORD_KEY_FIELD, String.join(",", primaryKeys));

        // Set partition path fields from partition keys
        List<String> partitionKeys = schema.partitionKeys();
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            config.setString(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys));
        } else {
            // For unpartitioned tables, use empty string
            config.setString(FlinkOptions.PARTITION_PATH_FIELD, "");
        }

        // Convert schema to RowType
        RowType rowType = toRowType(schema);

        // Create and return RowDataKeyGen using static factory method
        return RowDataKeyGen.instance(config, rowType);
    }
}
