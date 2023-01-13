/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.oceanbase.oms.logmessage.ByteString;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseAppendMetadataCollector;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseMetadataConverter;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.ververica.cdc.debezium.utils.TemporalConversions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from OceanBase object to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class RowDataOceanBaseDeserializationSchema
        implements OceanBaseDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that OceanBase record data into {@link RowData} consisted of physical
     * column values.
     */
    private final OceanBaseDeserializationRuntimeConverter physicalConverter;

    /** Whether the deserializer needs to handle metadata columns. */
    private final boolean hasMetadata;

    /**
     * A wrapped output collector which is used to append metadata columns after physical columns.
     */
    private final OceanBaseAppendMetadataCollector appendMetadataCollector;

    /** Returns a builder to build {@link RowDataOceanBaseDeserializationSchema}. */
    public static RowDataOceanBaseDeserializationSchema.Builder newBuilder() {
        return new RowDataOceanBaseDeserializationSchema.Builder();
    }

    RowDataOceanBaseDeserializationSchema(
            RowType physicalDataType,
            OceanBaseMetadataConverter[] metadataConverters,
            TypeInformation<RowData> resultTypeInfo,
            ZoneId serverTimeZone) {
        this.hasMetadata = checkNotNull(metadataConverters).length > 0;
        this.appendMetadataCollector = new OceanBaseAppendMetadataCollector(metadataConverters);
        this.physicalConverter = createConverter(checkNotNull(physicalDataType), serverTimeZone);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
    }

    @Override
    public void deserialize(OceanBaseRecord record, Collector<RowData> out) throws Exception {
        RowData physicalRow;
        if (record.isSnapshotRecord()) {
            physicalRow = (GenericRowData) physicalConverter.convert(record.getJdbcFields());
            physicalRow.setRowKind(RowKind.INSERT);
            emit(record, physicalRow, out);
        } else {
            switch (record.getOpt()) {
                case INSERT:
                    physicalRow =
                            (GenericRowData)
                                    physicalConverter.convert(record.getLogMessageFieldsAfter());
                    physicalRow.setRowKind(RowKind.INSERT);
                    emit(record, physicalRow, out);
                    break;
                case DELETE:
                    physicalRow =
                            (GenericRowData)
                                    physicalConverter.convert(record.getLogMessageFieldsBefore());
                    physicalRow.setRowKind(RowKind.DELETE);
                    emit(record, physicalRow, out);
                    break;
                case UPDATE:
                    physicalRow =
                            (GenericRowData)
                                    physicalConverter.convert(record.getLogMessageFieldsBefore());
                    physicalRow.setRowKind(RowKind.UPDATE_BEFORE);
                    emit(record, physicalRow, out);
                    physicalRow =
                            (GenericRowData)
                                    physicalConverter.convert(record.getLogMessageFieldsAfter());
                    physicalRow.setRowKind(RowKind.UPDATE_AFTER);
                    emit(record, physicalRow, out);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported log message record type: " + record.getOpt());
            }
        }
    }

    private void emit(OceanBaseRecord row, RowData physicalRow, Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }

        appendMetadataCollector.inputRecord = row;
        appendMetadataCollector.outputCollector = collector;
        appendMetadataCollector.collect(physicalRow);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    /** Builder class of {@link RowDataOceanBaseDeserializationSchema}. */
    public static class Builder {
        private RowType physicalRowType;
        private TypeInformation<RowData> resultTypeInfo;
        private OceanBaseMetadataConverter[] metadataConverters = new OceanBaseMetadataConverter[0];
        private ZoneId serverTimeZone = ZoneId.of("UTC");

        public RowDataOceanBaseDeserializationSchema.Builder setPhysicalRowType(
                RowType physicalRowType) {
            this.physicalRowType = physicalRowType;
            return this;
        }

        public RowDataOceanBaseDeserializationSchema.Builder setMetadataConverters(
                OceanBaseMetadataConverter[] metadataConverters) {
            this.metadataConverters = metadataConverters;
            return this;
        }

        public RowDataOceanBaseDeserializationSchema.Builder setResultTypeInfo(
                TypeInformation<RowData> resultTypeInfo) {
            this.resultTypeInfo = resultTypeInfo;
            return this;
        }

        public RowDataOceanBaseDeserializationSchema.Builder setServerTimeZone(
                ZoneId serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public RowDataOceanBaseDeserializationSchema build() {
            return new RowDataOceanBaseDeserializationSchema(
                    physicalRowType, metadataConverters, resultTypeInfo, serverTimeZone);
        }
    }

    private static OceanBaseDeserializationRuntimeConverter createConverter(
            LogicalType type, ZoneId serverTimeZone) {
        return wrapIntoNullableConverter(createNotNullConverter(type, serverTimeZone));
    }

    private static OceanBaseDeserializationRuntimeConverter wrapIntoNullableConverter(
            OceanBaseDeserializationRuntimeConverter converter) {
        return new OceanBaseDeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) throws Exception {
                if (object == null) {
                    return null;
                }
                return converter.convert(object);
            }
        };
    }

    public static OceanBaseDeserializationRuntimeConverter createNotNullConverter(
            LogicalType type, ZoneId serverTimeZone) {
        switch (type.getTypeRoot()) {
            case ROW:
                return createRowConverter((RowType) type, serverTimeZone);
            case NULL:
                return convertToNull();
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return convertToTinyInt();
            case SMALLINT:
                return convertToSmallInt();
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
                return convertToLocalTimeZoneTimestamp(serverTimeZone);
            case FLOAT:
                return convertToFloat();
            case DOUBLE:
                return convertToDouble();
            case CHAR:
            case VARCHAR:
                return convertToString();
            case BINARY:
                return convertToBinary();
            case VARBINARY:
                return convertToBytes();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter();
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static OceanBaseDeserializationRuntimeConverter createRowConverter(
            RowType rowType, ZoneId serverTimeZone) {
        final OceanBaseDeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(logicType -> createConverter(logicType, serverTimeZone))
                        .toArray(OceanBaseDeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) throws Exception {
                int arity = fieldNames.length;
                GenericRowData row = new GenericRowData(arity);
                Map<String, Object> fieldMap = (Map<String, Object>) object;
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    row.setField(i, fieldConverters[i].convert(fieldMap.get(fieldName)));
                }
                return row;
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToNull() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                return null;
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToBoolean() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof byte[]) {
                    return "1".equals(new String((byte[]) object, StandardCharsets.UTF_8));
                }
                return Boolean.parseBoolean(object.toString()) || "1".equals(object.toString());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return "1".equals(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToTinyInt() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                return Byte.parseByte(object.toString());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Byte.parseByte(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToSmallInt() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                return Short.parseShort(object.toString());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Short.parseShort(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToInt() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Integer) {
                    return object;
                } else if (object instanceof Long) {
                    return ((Long) object).intValue();
                } else if (object instanceof Date) {
                    return ((Date) object).toLocalDate().getYear();
                } else {
                    return Integer.parseInt(object.toString());
                }
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Integer.parseInt(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToLong() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Integer) {
                    return ((Integer) object).longValue();
                } else if (object instanceof Long) {
                    return object;
                } else {
                    return Long.parseLong(object.toString());
                }
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Long.parseLong(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToDouble() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Float) {
                    return ((Float) object).doubleValue();
                } else if (object instanceof Double) {
                    return object;
                } else {
                    return Double.parseDouble(object.toString());
                }
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Double.parseDouble(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToFloat() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Float) {
                    return object;
                } else if (object instanceof Double) {
                    return ((Double) object).floatValue();
                } else {
                    return Float.parseFloat(object.toString());
                }
            }

            @Override
            public Object convertChangeEvent(String string) {
                return Float.parseFloat(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToDate() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                return (int) TemporalConversions.toLocalDate(object).toEpochDay();
            }

            @Override
            public Object convertChangeEvent(String string) {
                return (int) Date.valueOf(string).toLocalDate().toEpochDay();
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToTime() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Long) {
                    return (int) ((Long) object / 1000_000);
                }
                return TemporalConversions.toLocalTime(object).toSecondOfDay() * 1000;
            }

            @Override
            public Object convertChangeEvent(String string) {
                return TemporalConversions.toLocalTime(Time.valueOf(string)).toSecondOfDay() * 1000;
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToTimestamp() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Timestamp) {
                    return TimestampData.fromLocalDateTime(((Timestamp) object).toLocalDateTime());
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + object
                                + "' of type "
                                + object.getClass().getName());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return TimestampData.fromLocalDateTime(Timestamp.valueOf(string).toLocalDateTime());
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(
            ZoneId serverTimeZone) {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof Timestamp) {
                    return TimestampData.fromInstant(
                            ((Timestamp) object)
                                    .toLocalDateTime()
                                    .atZone(serverTimeZone)
                                    .toInstant());
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + object
                                + "' of type "
                                + object.getClass().getName());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return TimestampData.fromInstant(
                        Timestamp.valueOf(string)
                                .toLocalDateTime()
                                .atZone(serverTimeZone)
                                .toInstant());
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToString() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                return StringData.fromString(object.toString());
            }

            @Override
            public Object convertChangeEvent(String string) {
                return StringData.fromString(string);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToBinary() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof byte[]) {
                    String str = new String((byte[]) object, StandardCharsets.US_ASCII);
                    return str.getBytes(StandardCharsets.UTF_8);
                } else if (object instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BINARY value type: " + object.getClass().getSimpleName());
                }
            }

            @Override
            public Object convertChangeEvent(String string) {
                try {
                    long v = Long.parseLong(string);
                    byte[] bytes = ByteBuffer.allocate(8).putLong(v).array();
                    int i = 0;
                    while (i < Long.BYTES - 1 && bytes[i] == 0) {
                        i++;
                    }
                    return Arrays.copyOfRange(bytes, i, Long.BYTES);
                } catch (NumberFormatException e) {
                    return string.getBytes(StandardCharsets.UTF_8);
                }
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter convertToBytes() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                if (object instanceof byte[]) {
                    return object;
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

            @Override
            public Object convertChangeEvent(String string) {
                return string.getBytes(StandardCharsets.UTF_8);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter createDecimalConverter(
            DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();

        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convertSnapshotEvent(Object object) {
                BigDecimal bigDecimal;
                if (object instanceof String) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Long) {
                    bigDecimal = new BigDecimal((Long) object);
                } else if (object instanceof BigInteger) {
                    bigDecimal = new BigDecimal((BigInteger) object);
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

            @Override
            public Object convertChangeEvent(String string) {
                return DecimalData.fromBigDecimal(new BigDecimal(string), precision, scale);
            }
        };
    }

    private static OceanBaseDeserializationRuntimeConverter createArrayConverter() {
        return new OceanBaseDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                String s;
                if (object instanceof ByteString) {
                    s = ((ByteString) object).toString(StandardCharsets.UTF_8.name());
                } else {
                    s = object.toString();
                }
                String[] strArray = s.split(",");
                StringData[] stringDataArray = new StringData[strArray.length];
                for (int i = 0; i < strArray.length; i++) {
                    stringDataArray[i] = StringData.fromString(strArray[i]);
                }
                return new GenericArrayData(stringDataArray);
            }
        };
    }
}
