/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypeDefaultVisitor;
import com.ververica.cdc.common.types.DateType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.DoubleType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.TinyIntType;
import com.ververica.cdc.common.types.VarCharType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.ververica.cdc.common.types.DataTypeChecks.getPrecision;
import static com.ververica.cdc.common.types.DataTypeChecks.getScale;

/** Utilities for conversion from source table to StarRocks table. */
public class StarRocksUtils {

    /** Convert a source table to {@link StarRocksTable}. */
    public static StarRocksTable toStarRocksTable(
            TableId tableId, Schema schema, TableCreateConfig tableCreateConfig) {
        if (schema.primaryKeys().isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Only support StarRocks primary key table, but the source table %s has no primary keys",
                            tableId));
        }

        // For StarRocks primary key table DDL, primary key columns must be defined before other
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

        List<StarRocksColumn> starRocksColumns = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            Column column = orderedColumns.get(i);
            StarRocksColumn.Builder builder =
                    new StarRocksColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(i)
                            .setColumnComment(column.getComment());
            toStarRocksDataType(column, builder);
            starRocksColumns.add(builder.build());
        }

        StarRocksTable.Builder tableBuilder =
                new StarRocksTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                        .setColumns(starRocksColumns)
                        .setTableKeys(schema.primaryKeys())
                        // use primary keys as distribution keys by default
                        .setDistributionKeys(schema.primaryKeys())
                        .setComment(schema.comment());
        if (tableCreateConfig.getNumBuckets().isPresent()) {
            tableBuilder.setNumBuckets(tableCreateConfig.getNumBuckets().get());
        }
        tableBuilder.setTableProperties(tableCreateConfig.getProperties());
        return tableBuilder.build();
    }

    /** Convert CDC data type to StarRocks data type. */
    public static void toStarRocksDataType(Column cdcColumn, StarRocksColumn.Builder builder) {
        CdcDataTypeTransformer dataTypeTransformer = new CdcDataTypeTransformer(builder);
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
    // StarRocks data types
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

    /** Max size of char type of StarRocks. */
    public static final int MAX_CHAR_SIZE = 255;

    /** Max size of varchar type of StarRocks. */
    public static final int MAX_VARCHAR_SIZE = 1048576;

    /** Transforms CDC {@link DataType} to StarRocks data type. */
    public static class CdcDataTypeTransformer
            extends DataTypeDefaultVisitor<StarRocksColumn.Builder> {

        private final StarRocksColumn.Builder builder;

        public CdcDataTypeTransformer(StarRocksColumn.Builder builder) {
            this.builder = builder;
        }

        @Override
        public StarRocksColumn.Builder visit(BooleanType booleanType) {
            builder.setDataType(BOOLEAN);
            builder.setNullable(booleanType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(TinyIntType tinyIntType) {
            builder.setDataType(TINYINT);
            builder.setNullable(tinyIntType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(SmallIntType smallIntType) {
            builder.setDataType(SMALLINT);
            builder.setNullable(smallIntType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(IntType intType) {
            builder.setDataType(INT);
            builder.setNullable(intType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(BigIntType bigIntType) {
            builder.setDataType(BIGINT);
            builder.setNullable(bigIntType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(FloatType floatType) {
            builder.setDataType(FLOAT);
            builder.setNullable(floatType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DoubleType doubleType) {
            builder.setDataType(DOUBLE);
            builder.setNullable(doubleType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DecimalType decimalType) {
            builder.setDataType(DECIMAL);
            builder.setNullable(decimalType.isNullable());
            builder.setColumnSize(decimalType.getPrecision());
            builder.setDecimalDigits(decimalType.getScale());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(CharType charType) {
            // CDC and StarRocks use different units for the length. It's the number
            // of characters in CDC, and the number of bytes in StarRocks. One chinese
            // character will use 3 bytes because it uses UTF-8, so the length of StarRocks
            // char type should be three times as that of CDC char type. Specifically, if
            // the length of StarRocks exceeds the MAX_CHAR_SIZE, map CDC char type to StarRocks
            // varchar type
            int length = charType.getLength();
            long starRocksLength = length * 3L;
            if (starRocksLength <= MAX_CHAR_SIZE) {
                builder.setDataType(CHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize((int) starRocksLength);
            } else {
                builder.setDataType(VARCHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize((int) Math.min(starRocksLength, MAX_VARCHAR_SIZE));
            }
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(VarCharType varCharType) {
            // CDC and StarRocks use different units for the length. It's the number
            // of characters in CDC, and the number of bytes in StarRocks. One chinese
            // character will use 3 bytes because it uses UTF-8, so the length of StarRocks
            // varchar type should be three times as that of CDC varchar type.
            int length = varCharType.getLength();
            long starRocksLength = length * 3L;
            builder.setDataType(VARCHAR);
            builder.setNullable(varCharType.isNullable());
            builder.setColumnSize((int) Math.min(starRocksLength, MAX_VARCHAR_SIZE));
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DateType dateType) {
            builder.setDataType(DATE);
            builder.setNullable(dateType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(TimestampType timestampType) {
            builder.setDataType(DATETIME);
            builder.setNullable(timestampType.isNullable());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
            builder.setDataType(DATETIME);
            builder.setNullable(localZonedTimestampType.isNullable());
            return builder;
        }

        @Override
        protected StarRocksColumn.Builder defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
        }
    }
}
