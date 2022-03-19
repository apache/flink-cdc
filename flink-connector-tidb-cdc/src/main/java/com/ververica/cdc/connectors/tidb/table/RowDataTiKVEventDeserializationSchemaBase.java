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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.debezium.utils.TemporalConversions;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of deserialization schema from TiKV RowValue (Snapshot or Change Event) to Flink
 * Table/SQL internal data structure {@link RowData}.
 */
public class RowDataTiKVEventDeserializationSchemaBase implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Whether the deserializer needs to handle metadata columns. */
    private final boolean hasMetadata;

    /** Information of the TiKV table. * */
    protected final TiTableInfo tableInfo;

    /**
     * A wrapped output collector which is used to append metadata columns after physical columns.
     */
    private final TiKVAppendMetadataCollector appendMetadataCollector;

    /**
     * Runtime converter that converts Tikv {@link Kvrpcpb.KvPair}s into {@link RowData} consisted
     * of physical column values.
     */
    protected final TidbDeserializationRuntimeConverter physicalConverter;

    public RowDataTiKVEventDeserializationSchemaBase(
            TiConfiguration tiConf,
            String database,
            String tableName,
            TiKVMetadataConverter[] metadataConverters,
            DataType producedDataType) {
        this.tableInfo = fetchTableInfo(tiConf, database, tableName);
        this.hasMetadata = checkNotNull(metadataConverters).length > 0;
        this.appendMetadataCollector = new TiKVAppendMetadataCollector(metadataConverters);
        this.physicalConverter = createConverter(checkNotNull(producedDataType.getLogicalType()));
    }

    private TiTableInfo fetchTableInfo(TiConfiguration tiConf, String database, String tableName) {
        try (final TiSession session = TiSession.create(tiConf)) {
            return session.getCatalog().getTable(database, tableName);
        } catch (final Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public void emit(
            TiKVMetadataConverter.TiKVRowValue inRecord,
            RowData physicalRow,
            Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }

        appendMetadataCollector.row = inRecord;
        appendMetadataCollector.outputCollector = collector;
        appendMetadataCollector.collect(physicalRow);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /** Creates a runtime converter which is null safe. */
    protected static TidbDeserializationRuntimeConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260).
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter which assuming input object is not null. */
    public static TidbDeserializationRuntimeConverter createNotNullConverter(LogicalType type) {

        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case NULL:
                return new TidbDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo schema,
                            org.tikv.common.types.DataType dataType) {
                        return null;
                    }
                };
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return new TidbDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo schema,
                            org.tikv.common.types.DataType dataType) {

                        return Byte.parseByte(object.toString());
                    }
                };
            case SMALLINT:
                return new TidbDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo schema,
                            org.tikv.common.types.DataType dataType) {
                        return Short.parseShort(object.toString());
                    }
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return convertToInt();
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return convertToLong();
            case DATE:
                return convertToDate();
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp();
            case FLOAT:
                return convertToFloat();
            case DOUBLE:
                return convertToDouble();
            case CHAR:
            case VARCHAR:
                return convertToString();
            case BINARY:
            case VARBINARY:
                return convertToBinary();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
                return new TidbDeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object object,
                            TiTableInfo tableInfo,
                            org.tikv.common.types.DataType dataType)
                            throws Exception {
                        String[] strArray = ((String) object).split(",");
                        StringData[] stringDataArray = new StringData[strArray.length];
                        for (int i = 0; i < strArray.length; i++) {
                            stringDataArray[i] = StringData.fromString(strArray[i]);
                        }
                        return new GenericArrayData(stringDataArray);
                    }
                };
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static TidbDeserializationRuntimeConverter convertToBoolean() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Boolean) {
                    return object;
                } else if (object instanceof Long) {
                    return (Long) object == 1;
                } else if (object instanceof Byte) {
                    return (byte) object == 1;
                } else if (object instanceof Short) {
                    return (short) object == 1;
                } else {
                    return Boolean.parseBoolean(object.toString());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToInt() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Integer) {
                    return object;
                } else if (object instanceof Long) {
                    return dataType.isUnsigned()
                            ? Integer.valueOf(Short.toUnsignedInt(((Long) object).shortValue()))
                            : ((Long) object).intValue();
                } else {
                    return Integer.parseInt(object.toString());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToLong() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Integer) {
                    return ((Integer) object).longValue();
                } else if (object instanceof Long) {
                    return object;
                } else {
                    return Long.parseLong(object.toString());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToDouble() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Float) {
                    return ((Float) object).doubleValue();
                } else if (object instanceof Double) {
                    return object;
                } else {
                    return Double.parseDouble(object.toString());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToFloat() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Float) {
                    return object;
                } else if (object instanceof Double) {
                    return ((Double) object).floatValue();
                } else {
                    return Float.parseFloat(object.toString());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToDate() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                return (int) TemporalConversions.toLocalDate(object).toEpochDay();
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToTime() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof Long) {
                    return (int) ((Long) object / 1000_000);
                }
                return TemporalConversions.toLocalTime(object).toSecondOfDay() * 1000;
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToTimestamp() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {

                switch (dataType.getType()) {
                    case TypeTimestamp:
                        if (object instanceof Timestamp) {
                            return TimestampData.fromInstant(((Timestamp) object).toInstant());
                        }
                    case TypeDatetime:
                        if (object instanceof Timestamp) {
                            return TimestampData.fromLocalDateTime(
                                    ((Timestamp) object).toLocalDateTime());
                        }
                    default:
                        throw new IllegalArgumentException(
                                "Unable to convert to TimestampData from unexpected value '"
                                        + object
                                        + "' of type "
                                        + object.getClass().getName());
                }
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToLocalTimeZoneTimestamp() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof String) {
                    String str = (String) object;
                    // TIMESTAMP_LTZ type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, ZoneId.of("UTC")));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + object
                                + "' of type "
                                + object.getClass().getName());
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToString() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof byte[]) {
                    return StringData.fromBytes((byte[]) object);
                }
                return StringData.fromString(object.toString());
            }
        };
    }

    private static TidbDeserializationRuntimeConverter convertToBinary() {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                if (object instanceof byte[]) {
                    return object;
                } else if (object instanceof String) {
                    return ((String) object).getBytes();
                } else if (object instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + object.getClass().getSimpleName());
                }
            }
        };
    }

    /** Deal with unsigned column's value. */
    public static Object dealUnsignedColumnValue(
            org.tikv.common.types.DataType dataType, Object object) {
        switch (dataType.getType()) {
            case TypeTiny:
                return Short.valueOf((short) Byte.toUnsignedInt(((Long) object).byteValue()));
            case TypeShort:
                return Integer.valueOf(Short.toUnsignedInt(((Long) object).shortValue()));
            case TypeInt24:
                return Integer.valueOf((((Long) object).intValue()) & 0xffffff);
            case TypeLong:
                return Long.valueOf(Integer.toUnsignedLong(((Long) object).intValue()));
            case TypeLonglong:
                return new BigDecimal(Long.toUnsignedString(((Long) object)));
            default:
                return object;
        }
    }

    private static TidbDeserializationRuntimeConverter createDecimalConverter(
            DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType) {
                BigDecimal bigDecimal;
                if (object instanceof String) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Long) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Double) {
                    bigDecimal = BigDecimal.valueOf((Double) object);
                } else if (object instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) object;
                } else {
                    throw new IllegalArgumentException(
                            "Unable to convert to decimal from unexpected value '"
                                    + object
                                    + "' of type "
                                    + object.getClass());
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }

    private static TidbDeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final TidbDeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(logicType -> createConverter(logicType))
                        .toArray(TidbDeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo tableInfo, org.tikv.common.types.DataType dataType)
                    throws Exception {
                int arity = fieldNames.length;
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];

                    TiColumnInfo columnInfo = tableInfo.getColumn(fieldName);
                    if (columnInfo == null) {
                        row.setField(i, null);
                    } else {
                        int offset = columnInfo.getOffset();
                        org.tikv.common.types.DataType type = columnInfo.getType();
                        if (type.isUnsigned()) {
                            ((Object[]) object)[offset] =
                                    dealUnsignedColumnValue(type, ((Object[]) object)[offset]);
                        }
                        Object convertedField =
                                convertField(
                                        fieldConverters[i],
                                        tableInfo,
                                        type,
                                        ((Object[]) object)[offset]);
                        row.setField(i, convertedField);
                        System.out.println(fieldName + "----" + i + "----" + convertedField);
                    }
                }
                return row;
            }
        };
    }

    private static Object convertField(
            TidbDeserializationRuntimeConverter fieldConverter,
            TiTableInfo tableInfo,
            org.tikv.common.types.DataType dataType,
            Object fieldValue)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, tableInfo, dataType);
        }
    }

    private static TidbDeserializationRuntimeConverter wrapIntoNullableConverter(
            TidbDeserializationRuntimeConverter converter) {
        return new TidbDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(
                    Object object, TiTableInfo schema, org.tikv.common.types.DataType dataType)
                    throws Exception {
                if (object == null) {
                    return null;
                }
                return converter.convert(object, schema, dataType);
            }
        };
    }
}
