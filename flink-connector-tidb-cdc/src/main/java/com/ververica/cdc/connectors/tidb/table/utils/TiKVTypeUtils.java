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

package com.ververica.cdc.connectors.tidb.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.StringType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static java.lang.String.format;

/**
 * Utils to deal the type mapping between TiKV and Flink SQL, this is inspired by TiFlink project.
 */
public class TiKVTypeUtils {

    private TiKVTypeUtils() {}

    public static boolean isIndexKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'i';
    }

    public static boolean isRecordKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'r';
    }

    /** The default Data Type mapping from TiKV to Flink SQL. */
    public static DataType getNullableFlinkType(org.tikv.common.types.DataType dataType) {
        boolean unsigned = dataType.isUnsigned();
        int length = (int) dataType.getLength();
        switch (dataType.getType()) {
            case TypeBit:
                return DataTypes.BOOLEAN();
            case TypeTiny:
                if (length == 1) {
                    return DataTypes.BOOLEAN();
                }
                return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
            case TypeYear:
            case TypeShort:
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case TypeInt24:
            case TypeLong:
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case TypeLonglong:
                return unsigned ? DataTypes.DECIMAL(length, 0) : DataTypes.BIGINT();
            case TypeFloat:
                return DataTypes.FLOAT();
            case TypeDouble:
                return DataTypes.DOUBLE();
            case TypeNull:
                return DataTypes.NULL();
            case TypeDatetime:
            case TypeTimestamp:
                return DataTypes.TIMESTAMP();
            case TypeDate:
            case TypeNewDate:
                return DataTypes.DATE();
            case TypeDuration:
                return DataTypes.TIME();
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeBlob:
            case TypeVarString:
            case TypeString:
            case TypeVarchar:
            case TypeLongBlob:
                Preconditions.checkArgument(
                        dataType.getLength() <= Integer.MAX_VALUE, "Field length exceed maximum");
                if (dataType instanceof StringType) {
                    return DataTypes.VARCHAR((int) dataType.getLength());
                }
                return DataTypes.VARBINARY((int) dataType.getLength());
            case TypeJSON:
            case TypeEnum:
            case TypeSet:
                return DataTypes.STRING();
            case TypeDecimal:
            case TypeNewDecimal:
                return DataTypes.DECIMAL(length, dataType.getDecimal());
            case TypeGeometry:
            default:
                throw new IllegalArgumentException(
                        format("can not get flink datatype by tikv type: %s", dataType));
        }
    }

    public static DataType getFlinkType(final org.tikv.common.types.DataType dataType) {
        final DataType typ = getNullableFlinkType(dataType);
        return dataType.isNotNull() ? typ.notNull() : typ;
    }

    /**
     * Transform TiKV java object to Flink java object by given Flink Datatype.
     *
     * @param object TiKV java object
     * @param dataType Flink datatype
     */
    public static Optional<Object> getObjectWithDataType(
            Object object, DataType dataType, DateTimeFormatter formatter) {
        if (object == null) {
            return Optional.empty();
        }
        Class<?> conversionClass = dataType.getConversionClass();
        if (dataType.getConversionClass() == object.getClass()) {
            return Optional.of(object);
        }
        switch (conversionClass.getSimpleName()) {
            case "String":
                if (object instanceof byte[]) {
                    object = new String((byte[]) object);
                } else if (object instanceof Timestamp) {
                    Timestamp timestamp = (Timestamp) object;
                    object =
                            formatter == null
                                    ? timestamp.toString()
                                    : timestamp.toLocalDateTime().format(formatter);
                } else {
                    object = object.toString();
                }
                break;
            case "Integer":
                object =
                        (int)
                                (long)
                                        getObjectWithDataType(object, DataTypes.BIGINT(), formatter)
                                                .get();
                break;
            case "Long":
                if (object instanceof LocalDate) {
                    object = ((LocalDate) object).toEpochDay();
                } else if (object instanceof LocalDateTime) {
                    object = Timestamp.valueOf(((LocalDateTime) object)).getTime();
                } else if (object instanceof LocalTime) {
                    object = ((LocalTime) object).toNanoOfDay();
                }
                break;
            case "LocalDate":
                if (object instanceof Date) {
                    object = ((Date) object).toLocalDate();
                } else if (object instanceof String) {
                    object = LocalDate.parse((String) object);
                } else if (object instanceof Long || object instanceof Integer) {
                    object = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                }
                break;
            case "LocalDateTime":
                if (object instanceof Timestamp) {
                    object = ((Timestamp) object).toLocalDateTime();
                } else if (object instanceof String) {
                    String timeString = (String) object;
                    object =
                            formatter == null
                                    ? LocalDateTime.parse(timeString)
                                    : LocalDateTime.parse(timeString, formatter);
                } else if (object instanceof Long) {
                    object = new Timestamp(((Long) object) / 1000).toLocalDateTime();
                }
                break;
            case "LocalTime":
                if (object instanceof Long || object instanceof Integer) {
                    object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
                }
                break;
            default:
                object = null;
        }
        return Optional.of(object);
    }

    public static Optional<Object> getObjectWithDataType(
            final Object object, final DataType dataType) {
        return getObjectWithDataType(object, dataType, null);
    }

    public static Object[] toObjects(final RowData row, final FieldGetter[] fieldGetters) {
        final Object[] res = new Object[row.getArity()];
        for (int i = 0; i < res.length; i++) {
            res[i] = fieldGetters[i].getFieldOrNull(row);
            if (res[i] instanceof StringData) {
                res[i] = res[i].toString();
            } else if (res[i] instanceof TimestampData) {
                res[i] = ((TimestampData) res[i]).toTimestamp();
            } else if (res[i] instanceof DecimalData) {
                res[i] = ((DecimalData) res[i]).toBigDecimal();
            }
        }
        return res;
    }

    public static Object[] getObjectsWithDataTypes(
            final Object[] objects, final TiTableInfo tableInfo) {
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] == null) {
                continue;
            }
            objects[i] =
                    toRowDataType(
                            getObjectWithDataType(
                                            objects[i],
                                            getFlinkType(tableInfo.getColumn(i).getType()))
                                    .get(),
                            getFlinkType(tableInfo.getColumn(i).getType()));
        }
        return objects;
    }

    /** Transform Row type to GenericRowData type. */
    public static Object toRowDataType(Object object, DataType dataType) {
        Object result = object;
        if (object == null) {
            return null;
        }
        switch (object.getClass().getSimpleName()) {
            case "String":
                result = StringData.fromString(object.toString());
                break;
            case "BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) object;
                // TODO improve the conversion code
                int precision = ((DecimalType) dataType.getLogicalType()).getPrecision();
                int scale = ((DecimalType) dataType.getLogicalType()).getScale();
                result = DecimalData.fromBigDecimal(bigDecimal, precision, scale);
                break;
            case "LocalDate":
                LocalDate localDate = (LocalDate) object;
                result = (int) localDate.toEpochDay();
                break;
            case "LocalDateTime":
                result = TimestampData.fromLocalDateTime((LocalDateTime) object);
                break;
            case "LocalTime":
                LocalTime localTime = (LocalTime) object;
                result = (int) (localTime.toNanoOfDay() / (1000 * 1000));
                break;
            default:
                // pass code style
                break;
        }
        return result;
    }

    public static boolean isIntType(final org.tikv.common.types.DataType tp) {
        switch (tp.getType()) {
            case TypeBit:
            case TypeInt24:
            case TypeLong:
            case TypeShort:
                return true;
            default:
                return false;
        }
    }

    public static FieldGetter createFieldGetter(final LogicalType fieldType, final int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
                final int decimalScale = LogicalTypeChecks.getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case DATE:
                fieldGetter =
                        (row -> {
                            final int days = row.getInt(fieldPos);
                            final Instant instant = Instant.EPOCH.plus(days, ChronoUnit.DAYS);
                            return Timestamp.from(instant);
                        });
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = LogicalTypeChecks.getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = LogicalTypeChecks.getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter =
                        createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
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

    public static String toMySQLTypeString(final DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case VARBINARY:
                return toVarTypeString(dataType, "VARBINARY", "MEDIUMBLOB", "LONGBLOB");
            case VARCHAR:
                return toVarTypeString(dataType, "VARCHAR", "MEDIUMTEXT", "LONGTEXT");
            case BIGINT:
            case INTEGER:
            case DOUBLE:
            case FLOAT:
            case CHAR:
            case BOOLEAN:
            case DECIMAL:
            case SMALLINT:
            case TINYINT:
            case BINARY:
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIME_WITHOUT_TIME_ZONE:
                return dataType.toString();
            default:
                throw new IllegalArgumentException(
                        "Unsupported field type: " + dataType.toString());
        }
    }

    private static String toVarTypeString(
            final DataType dataType,
            final String varType,
            final String mediumType,
            final String longType) {
        final int length = ((VarCharType) dataType.getLogicalType()).getLength();
        final boolean isNullable = ((VarCharType) dataType.getLogicalType()).isNullable();

        if (length <= 65535) {
            return String.format("%s(%d)", varType, Math.max(length, 0))
                    + (isNullable ? "" : " NOT NULL");
        } else if (length <= 16777215) {
            return mediumType + (isNullable ? "" : " NOT NULL");
        } else {
            return longType + (isNullable ? "" : " NOT NULL");
        }
    }
}
