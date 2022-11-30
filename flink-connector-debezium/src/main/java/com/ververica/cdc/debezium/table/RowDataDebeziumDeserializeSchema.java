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

package com.ververica.cdc.debezium.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public final class RowDataDebeziumDeserializeSchema
        implements DebeziumDeserializationSchema<RowData> {
    private static final long serialVersionUID = 2L;

    /** Custom validator to validate the row value. */
    public interface ValueValidator extends Serializable {
        void validate(RowData rowData, RowKind rowKind) throws Exception;
    }

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that converts Kafka {@link SourceRecord}s into {@link RowData} consisted of
     * physical column values.
     */
    private final DeserializationRuntimeConverter physicalConverter;

    /** Whether the deserializer needs to handle metadata columns. */
    private final boolean hasMetadata;

    /**
     * A wrapped output collector which is used to append metadata columns after physical columns.
     */
    private final AppendMetadataCollector appendMetadataCollector;

    /** Validator to validate the row value. */
    private final ValueValidator validator;

    /** Changelog Mode to use for encoding changes in Flink internal data structure. */
    private final DebeziumChangelogMode changelogMode;

    /** Returns a builder to build {@link RowDataDebeziumDeserializeSchema}. */
    public static Builder newBuilder() {
        return new Builder();
    }

    RowDataDebeziumDeserializeSchema(
            RowType physicalDataType,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> resultTypeInfo,
            ValueValidator validator,
            ZoneId serverTimeZone,
            DeserializationRuntimeConverterFactory userDefinedConverterFactory,
            DebeziumChangelogMode changelogMode) {
        this.hasMetadata = checkNotNull(metadataConverters).length > 0;
        this.appendMetadataCollector = new AppendMetadataCollector(metadataConverters);
        this.physicalConverter =
                createConverter(
                        checkNotNull(physicalDataType),
                        serverTimeZone,
                        userDefinedConverterFactory);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.validator = checkNotNull(validator);
        this.changelogMode = checkNotNull(changelogMode);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            GenericRowData insert = extractAfterRow(value, valueSchema);
            validator.validate(insert, RowKind.INSERT);
            insert.setRowKind(RowKind.INSERT);
            emit(record, insert, out);
        } else if (op == Envelope.Operation.DELETE) {
            GenericRowData delete = extractBeforeRow(value, valueSchema);
            validator.validate(delete, RowKind.DELETE);
            delete.setRowKind(RowKind.DELETE);
            emit(record, delete, out);
        } else {
            if (changelogMode == DebeziumChangelogMode.ALL) {
                GenericRowData before = extractBeforeRow(value, valueSchema);
                validator.validate(before, RowKind.UPDATE_BEFORE);
                before.setRowKind(RowKind.UPDATE_BEFORE);
                emit(record, before, out);
            }

            GenericRowData after = extractAfterRow(value, valueSchema);
            validator.validate(after, RowKind.UPDATE_AFTER);
            after.setRowKind(RowKind.UPDATE_AFTER);
            emit(record, after, out);
        }
    }

    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return (GenericRowData) physicalConverter.convert(after, afterSchema);
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return (GenericRowData) physicalConverter.convert(before, beforeSchema);
    }

    private void emit(SourceRecord inRecord, RowData physicalRow, Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }

        appendMetadataCollector.inputRecord = inRecord;
        appendMetadataCollector.outputCollector = collector;
        appendMetadataCollector.collect(physicalRow);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    // -------------------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------------------

    /** Builder of {@link RowDataDebeziumDeserializeSchema}. */
    public static class Builder {
        private RowType physicalRowType;
        private TypeInformation<RowData> resultTypeInfo;
        private MetadataConverter[] metadataConverters = new MetadataConverter[0];
        private ValueValidator validator = (rowData, rowKind) -> {};
        private ZoneId serverTimeZone = ZoneId.of("UTC");
        private DeserializationRuntimeConverterFactory userDefinedConverterFactory =
                DeserializationRuntimeConverterFactory.DEFAULT;
        private DebeziumChangelogMode changelogMode = DebeziumChangelogMode.ALL;

        public Builder setPhysicalRowType(RowType physicalRowType) {
            this.physicalRowType = physicalRowType;
            return this;
        }

        public Builder setMetadataConverters(MetadataConverter[] metadataConverters) {
            this.metadataConverters = metadataConverters;
            return this;
        }

        public Builder setResultTypeInfo(TypeInformation<RowData> resultTypeInfo) {
            this.resultTypeInfo = resultTypeInfo;
            return this;
        }

        public Builder setValueValidator(ValueValidator validator) {
            this.validator = validator;
            return this;
        }

        public Builder setServerTimeZone(ZoneId serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder setUserDefinedConverterFactory(
                DeserializationRuntimeConverterFactory userDefinedConverterFactory) {
            this.userDefinedConverterFactory = userDefinedConverterFactory;
            return this;
        }

        public Builder setChangelogMode(DebeziumChangelogMode changelogMode) {
            this.changelogMode = changelogMode;
            return this;
        }

        public RowDataDebeziumDeserializeSchema build() {
            return new RowDataDebeziumDeserializeSchema(
                    physicalRowType,
                    metadataConverters,
                    resultTypeInfo,
                    validator,
                    serverTimeZone,
                    userDefinedConverterFactory,
                    changelogMode);
        }
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /** Creates a runtime converter which is null safe. */
    private static DeserializationRuntimeConverter createConverter(
            LogicalType type,
            ZoneId serverTimeZone,
            DeserializationRuntimeConverterFactory userDefinedConverterFactory) {
        return wrapIntoNullableConverter(
                createNotNullConverter(type, serverTimeZone, userDefinedConverterFactory));
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260).
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter which assuming input object is not null. */
    public static DeserializationRuntimeConverter createNotNullConverter(
            LogicalType type,
            ZoneId serverTimeZone,
            DeserializationRuntimeConverterFactory userDefinedConverterFactory) {
        // user defined converter has a higher resolve order
        Optional<DeserializationRuntimeConverter> converter =
                userDefinedConverterFactory.createUserDefinedConverter(type, serverTimeZone);
        if (converter.isPresent()) {
            return converter.get();
        }

        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case NULL:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return null;
                    }
                };
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return Byte.parseByte(dbzObj.toString());
                    }
                };
            case SMALLINT:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return Short.parseShort(dbzObj.toString());
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
                return convertToTimestamp(serverTimeZone);
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
            case VARBINARY:
                return convertToBinary();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter(
                        (RowType) type, serverTimeZone, userDefinedConverterFactory);
            case ARRAY:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DeserializationRuntimeConverter convertToBoolean() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Boolean) {
                    return dbzObj;
                } else if (dbzObj instanceof Byte) {
                    return (byte) dbzObj == 1;
                } else if (dbzObj instanceof Short) {
                    return (short) dbzObj == 1;
                } else {
                    return Boolean.parseBoolean(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToInt() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return dbzObj;
                } else if (dbzObj instanceof Long) {
                    return ((Long) dbzObj).intValue();
                } else {
                    return Integer.parseInt(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToLong() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return ((Integer) dbzObj).longValue();
                } else if (dbzObj instanceof Long) {
                    return dbzObj;
                } else {
                    return Long.parseLong(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToDouble() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return ((Float) dbzObj).doubleValue();
                } else if (dbzObj instanceof Double) {
                    return dbzObj;
                } else {
                    return Double.parseDouble(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToFloat() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return dbzObj;
                } else if (dbzObj instanceof Double) {
                    return ((Double) dbzObj).floatValue();
                } else {
                    return Float.parseFloat(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToDate() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
            }
        };
    }

    private static DeserializationRuntimeConverter convertToTime() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000);
                        case NanoTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000_000);
                    }
                } else if (dbzObj instanceof Integer) {
                    return dbzObj;
                }
                // get number of milliseconds of the day
                return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
            }
        };
    }

    private static DeserializationRuntimeConverter convertToTimestamp(ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            return TimestampData.fromEpochMillis((Long) dbzObj);
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    micro / 1000, (int) (micro % 1000 * 1000));
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    nano / 1000_000, (int) (nano % 1000_000));
                    }
                }
                LocalDateTime localDateTime =
                        TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
                return TimestampData.fromLocalDateTime(localDateTime);
            }
        };
    }

    private static DeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(
            ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    // TIMESTAMP_LTZ type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, serverTimeZone));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToString() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return StringData.fromString(dbzObj.toString());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToBinary() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof byte[]) {
                    return dbzObj;
                } else if (dbzObj instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                BigDecimal bigDecimal;
                if (dbzObj instanceof byte[]) {
                    // decimal.handling.mode=precise
                    bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
                } else if (dbzObj instanceof String) {
                    // decimal.handling.mode=string
                    bigDecimal = new BigDecimal((String) dbzObj);
                } else if (dbzObj instanceof Double) {
                    // decimal.handling.mode=double
                    bigDecimal = BigDecimal.valueOf((Double) dbzObj);
                } else {
                    if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                        SpecialValueDecimal decimal =
                                VariableScaleDecimal.toLogical((Struct) dbzObj);
                        bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
                    } else {
                        // fallback to string
                        bigDecimal = new BigDecimal(dbzObj.toString());
                    }
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }

    private static DeserializationRuntimeConverter createRowConverter(
            RowType rowType,
            ZoneId serverTimeZone,
            DeserializationRuntimeConverterFactory userDefinedConverterFactory) {
        final DeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(
                                logicType ->
                                        createConverter(
                                                logicType,
                                                serverTimeZone,
                                                userDefinedConverterFactory))
                        .toArray(DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                Struct struct = (Struct) dbzObj;
                int arity = fieldNames.length;
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    Field field = schema.field(fieldName);
                    if (field == null) {
                        row.setField(i, null);
                    } else {
                        Object fieldValue = struct.getWithoutDefault(fieldName);
                        Schema fieldSchema = schema.field(fieldName).schema();
                        Object convertedField =
                                convertField(fieldConverters[i], fieldValue, fieldSchema);
                        row.setField(i, convertedField);
                    }
                }
                return row;
            }
        };
    }

    private static Object convertField(
            DeserializationRuntimeConverter fieldConverter, Object fieldValue, Schema fieldSchema)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, fieldSchema);
        }
    }

    private static DeserializationRuntimeConverter wrapIntoNullableConverter(
            DeserializationRuntimeConverter converter) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                if (dbzObj == null) {
                    return null;
                }
                return converter.convert(dbzObj, schema);
            }
        };
    }
}
