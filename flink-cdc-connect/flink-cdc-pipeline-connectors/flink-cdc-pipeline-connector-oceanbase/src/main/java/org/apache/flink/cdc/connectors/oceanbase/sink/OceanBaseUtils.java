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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** Utilities for conversion from source table to OceanBase table. */
public class OceanBaseUtils {

    /** Convert a source table to {@link OceanBaseTable}. */
    public static OceanBaseTable toOceanBaseTable(TableId tableId, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Only support OceanBase primary key table, but the source table %s has no primary keys",
                            tableId));
        }

        // For OceanBase primary key table DDL, primary key columns must be defined before other
        // columns,
        // so reorder the columns in the source schema to make primary key columns at the front
        List<Column> orderedColumns = new ArrayList<>();
        for (String primaryKey : schema.primaryKeys()) {
            orderedColumns.add(schema.getColumn(primaryKey).get());
        }
        for (Column column : schema.getColumns()) {
            if (!schema.primaryKeys().contains(column.getName())) {
                orderedColumns.add(column);
            }
        }

        int primaryKeyCount = schema.primaryKeys().size();
        List<OceanBaseColumn> oceanBaseColumns = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            Column column = orderedColumns.get(i);
            OceanBaseColumn.Builder builder =
                    new OceanBaseColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(i)
                            .setColumnComment(column.getComment());
            toOceanBaseDataType(column, i < primaryKeyCount, builder);
            oceanBaseColumns.add(builder.build());
        }

        OceanBaseTable.Builder tableBuilder =
                new OceanBaseTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(OceanBaseTable.TableType.PRIMARY_KEY)
                        .setColumns(oceanBaseColumns)
                        .setTableKeys(schema.primaryKeys())
                        // use primary keys as distribution keys by default
                        .setDistributionKeys(schema.primaryKeys())
                        .setComment(schema.comment());
        return tableBuilder.build();
    }

    /** Convert CDC data type to OceanBase data type. */
    public static void toOceanBaseDataType(
            Column cdcColumn, boolean isPrimaryKeys, OceanBaseColumn.Builder builder) {
        CdcDataTypeTransformer dataTypeTransformer =
                new CdcDataTypeTransformer(isPrimaryKeys, builder);
        cdcColumn.getType().accept(dataTypeTransformer);
    }

    /** Format DATE type data. */
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position.
     *
     * @param fieldType the element type of the RecordData
     * @param fieldPos the element position of the RecordData
     * @param zoneId the time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE
     *     </code>
     */
    public static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case TINYINT:
                fieldGetter = record -> record.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = record -> record.getShort(fieldPos);
                break;
            case INTEGER:
                fieldGetter = record -> record.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = record -> record.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getDouble(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                        .toBigDecimal();
                break;
            case CHAR:
            case VARCHAR:
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case DATE:
                fieldGetter =
                        record ->
                                LocalDate.ofEpochDay(record.getInt(fieldPos))
                                        .format(DATE_FORMATTER);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                ZonedDateTime.ofInstant(
                                                record.getLocalZonedTimestampData(
                                                                fieldPos, getPrecision(fieldType))
                                                        .toInstant(),
                                                zoneId)
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support data type " + fieldType.getTypeRoot());
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

    // ------------------------------------------------------------------------------------------
    // OceanBase data types
    // ------------------------------------------------------------------------------------------

    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String LARGEINT = "BIGINT UNSIGNED";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String JSON = "JSON";

    /** Max size of char type of OceanBase. */
    public static final int MAX_CHAR_SIZE = 255;

    /** Max size of varchar type of OceanBase. */
    public static final int MAX_VARCHAR_SIZE = 1048576;

    /** Transforms CDC {@link DataType} to OceanBase data type. */
    public static class CdcDataTypeTransformer
            extends DataTypeDefaultVisitor<OceanBaseColumn.Builder> {

        private final OceanBaseColumn.Builder builder;
        private final boolean isPrimaryKeys;

        public CdcDataTypeTransformer(boolean isPrimaryKeys, OceanBaseColumn.Builder builder) {
            this.isPrimaryKeys = isPrimaryKeys;
            this.builder = builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(BooleanType booleanType) {
            builder.setDataType(BOOLEAN);
            builder.setNullable(booleanType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(TinyIntType tinyIntType) {
            builder.setDataType(TINYINT);
            builder.setNullable(tinyIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(SmallIntType smallIntType) {
            builder.setDataType(SMALLINT);
            builder.setNullable(smallIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(IntType intType) {
            builder.setDataType(INT);
            builder.setNullable(intType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(BigIntType bigIntType) {
            builder.setDataType(BIGINT);
            builder.setNullable(bigIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(FloatType floatType) {
            builder.setDataType(FLOAT);
            builder.setNullable(floatType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DoubleType doubleType) {
            builder.setDataType(DOUBLE);
            builder.setNullable(doubleType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DecimalType decimalType) {
            // OceanBase does not support Decimal as primary key, so decimal should be cast to
            // VARCHAR.
            if (!isPrimaryKeys) {
                builder.setDataType(DECIMAL);
                builder.setColumnSize(decimalType.getPrecision());
                builder.setDecimalDigits(decimalType.getScale());
            } else {
                builder.setDataType(VARCHAR);
                // For a DecimalType with precision N, we may need N + 1 or N + 2 characters to
                // store it as a
                // string (one for negative sign, and one for decimal point)
                builder.setColumnSize(
                        Math.min(
                                decimalType.getScale() != 0
                                        ? decimalType.getPrecision() + 2
                                        : decimalType.getPrecision() + 1,
                                MAX_VARCHAR_SIZE));
            }
            builder.setNullable(decimalType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(CharType charType) {
            // CDC and OceanBase use different units for the length. It's the number
            // of characters in CDC, and the number of bytes in OceanBase. One chinese
            // character will use 3 bytes because it uses UTF-8, so the length of OceanBase
            // char type should be three times as that of CDC char type. Specifically, if
            // the length of OceanBase exceeds the MAX_CHAR_SIZE, map CDC char type to OceanBase
            // varchar type
            int length = charType.getLength();
            long oceanBaseLength = length * 3L;
            // In the OceanBase, The primary key columns can be any of the following data types:
            // BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, STRING, VARCHAR, DATE, and
            // DATETIME, But it doesn't include CHAR. When a char type appears in the primary key of
            // MySQL, creating a table in OceanBase requires conversion to varchar type.
            if (oceanBaseLength <= MAX_CHAR_SIZE && !isPrimaryKeys) {
                builder.setDataType(CHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize((int) oceanBaseLength);
            } else {
                builder.setDataType(VARCHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize((int) Math.min(oceanBaseLength, MAX_VARCHAR_SIZE));
            }
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(VarCharType varCharType) {
            // CDC and OceanBase use different units for the length. It's the number
            // of characters in CDC, and the number of bytes in OceanBase. One chinese
            // character will use 3 bytes because it uses UTF-8, so the length of OceanBase
            // varchar type should be three times as that of CDC varchar type.
            int length = varCharType.getLength();
            long oceanBaseLength = length * 3L;
            builder.setDataType(VARCHAR);
            builder.setNullable(varCharType.isNullable());
            builder.setColumnSize((int) Math.min(oceanBaseLength, MAX_VARCHAR_SIZE));
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DateType dateType) {
            builder.setDataType(DATE);
            builder.setNullable(dateType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(TimestampType timestampType) {
            builder.setDataType(DATETIME);
            builder.setNullable(timestampType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
            builder.setDataType(DATETIME);
            builder.setNullable(localZonedTimestampType.isNullable());
            return builder;
        }

        @Override
        protected OceanBaseColumn.Builder defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
        }
    }
}
