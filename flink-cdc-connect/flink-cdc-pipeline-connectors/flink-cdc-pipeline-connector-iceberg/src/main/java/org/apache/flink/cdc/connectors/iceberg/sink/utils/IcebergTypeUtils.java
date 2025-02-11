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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.DecimalUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;

/** Util class for {@link IcebergDataSink}. */
public class IcebergTypeUtils {

    /** Convert column from CDC framework to Iceberg framework. */
    public static Types.NestedField convertCDCColumnToIcebergField(
            int index, PhysicalColumn column) {
        DataType dataType = column.getType();
        return Types.NestedField.of(
                index,
                dataType.isNullable(),
                column.getName(),
                convertCDCTypeToIcebergType(dataType),
                column.getComment());
    }

    /** Convert data type from CDC framework to Iceberg framework. */
    public static Type convertCDCTypeToIcebergType(DataType type) {
        // ordered by type root definition
        List<DataType> children = type.getChildren();
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case CHAR:
                return new Types.StringType();
            case VARCHAR:
                return new Types.StringType();
            case BOOLEAN:
                return new Types.BooleanType();
            case BINARY:
                return new Types.BinaryType();
            case VARBINARY:
                return new Types.BinaryType();
            case DECIMAL:
                return Types.DecimalType.of(precision, scale);
            case TINYINT:
                return new Types.IntegerType();
            case SMALLINT:
                return new Types.IntegerType();
            case INTEGER:
                return new Types.IntegerType();
            case DATE:
                return new Types.DateType();
            case TIME_WITHOUT_TIME_ZONE:
                return Types.TimeType.get();
            case BIGINT:
                return new Types.LongType();
            case FLOAT:
                return new Types.FloatType();
            case DOUBLE:
                return new Types.DoubleType();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            case TIMESTAMP_WITH_TIME_ZONE:
                return Types.TimestampType.withZone();
            case ARRAY:
                throw new IllegalArgumentException("Illegal type: " + type);
            case MAP:
                throw new IllegalArgumentException("Illegal type: " + type);
            case ROW:
                throw new IllegalArgumentException("Illegal type: " + type);
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /** Create a {@link RecordData.FieldGetter} for the given {@link DataType}. */
    public static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
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
                final int decimalPrecision = DataTypeChecks.getPrecision(fieldType);
                final int decimalScale = DataTypeChecks.getScale(fieldType);
                fieldGetter =
                        row -> {
                            DecimalData decimalData =
                                    row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                            return DecimalUtil.toReusedFixLengthBytes(
                                    decimalPrecision,
                                    decimalScale,
                                    decimalData.toBigDecimal(),
                                    new byte[TypeUtil.decimalRequiredBytes(decimalPrecision)]);
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
            case TIME_WITHOUT_TIME_ZONE:
                // Time in RowData is in milliseconds (Integer), while iceberg's time is
                // microseconds
                // (Long).
                fieldGetter = row -> ((long) row.getInt(fieldPos)) * 1_000;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        (row) -> {
                            LocalDateTime localDateTime =
                                    row.getTimestamp(
                                                    fieldPos,
                                                    DataTypeChecks.getPrecision(fieldType))
                                            .toLocalDateTime();
                            return DateTimeUtil.microsFromTimestamp(localDateTime);
                        };
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        row ->
                                DateTimeUtil.microsFromInstant(
                                        row.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
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
}
