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

package com.ververica.cdc.debezium.event;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Debezium event deserializer for {@link SourceRecord}. */
@Internal
public abstract class DebeziumEventDeserializationSchema extends SourceRecordEventDeserializer
        implements DebeziumDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(DebeziumEventDeserializationSchema.class);

    private static final Map<DataType, DeserializationRuntimeConverter> CONVERTERS =
            new ConcurrentHashMap<>();

    /** The schema data type inference. */
    protected final SchemaDataTypeInference schemaDataTypeInference;

    /** Changelog Mode to use for encoding changes in Flink internal data structure. */
    protected final DebeziumChangelogMode changelogMode;

    public DebeziumEventDeserializationSchema(
            SchemaDataTypeInference schemaDataTypeInference, DebeziumChangelogMode changelogMode) {
        this.schemaDataTypeInference = schemaDataTypeInference;
        this.changelogMode = changelogMode;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        deserialize(record).forEach(out::collect);
    }

    @Override
    public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        TableId tableId = getTableId(record);

        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Map<String, String> meta = getMetadata(record);

        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            RecordData after = extractAfterDataRecord(value, valueSchema);
            return Collections.singletonList(DataChangeEvent.insertEvent(tableId, after, meta));
        } else if (op == Envelope.Operation.DELETE) {
            RecordData before = extractBeforeDataRecord(value, valueSchema);
            return Collections.singletonList(DataChangeEvent.deleteEvent(tableId, before, meta));
        } else if (op == Envelope.Operation.UPDATE) {
            RecordData after = extractAfterDataRecord(value, valueSchema);
            if (changelogMode == DebeziumChangelogMode.ALL) {
                RecordData before = extractBeforeDataRecord(value, valueSchema);
                return Collections.singletonList(
                        DataChangeEvent.updateEvent(tableId, before, after, meta));
            }
            return Collections.singletonList(
                    DataChangeEvent.updateEvent(tableId, null, after, meta));
        } else {
            LOG.trace("Received {} operation, skip", op);
            return Collections.emptyList();
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    private RecordData extractBeforeDataRecord(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = fieldSchema(valueSchema, Envelope.FieldName.BEFORE);
        Struct beforeValue = fieldStruct(value, Envelope.FieldName.BEFORE);
        return extractDataRecord(beforeValue, beforeSchema);
    }

    private RecordData extractAfterDataRecord(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = fieldSchema(valueSchema, Envelope.FieldName.AFTER);
        Struct afterValue = fieldStruct(value, Envelope.FieldName.AFTER);
        return extractDataRecord(afterValue, afterSchema);
    }

    private RecordData extractDataRecord(Struct value, Schema valueSchema) throws Exception {
        DataType dataType = schemaDataTypeInference.infer(value, valueSchema);
        return (RecordData) getOrCreateConverter(dataType).convert(value, valueSchema);
    }

    private DeserializationRuntimeConverter getOrCreateConverter(DataType type) {
        return CONVERTERS.computeIfAbsent(type, this::createConverter);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /** Creates a runtime converter which is null safe. */
    private DeserializationRuntimeConverter createConverter(DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260).
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter which assuming input object is not null. */
    protected DeserializationRuntimeConverter createNotNullConverter(DataType type) {
        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return this::convertToByte;
            case SMALLINT:
                return this::convertToShort;
            case INTEGER:
                return this::convertToInt;
            case BIGINT:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToLocalTimeZoneTimestamp;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBinary;
            case DECIMAL:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return convertToDecimal((DecimalType) type, dbzObj, schema);
                    }
                };
            case ROW:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        return convertToRecord((RowType) type, dbzObj, schema);
                    }
                };
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    protected Object convertToBoolean(Object dbzObj, Schema schema) {
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

    protected Object convertToByte(Object dbzObj, Schema schema) {
        return Byte.parseByte(dbzObj.toString());
    }

    protected Object convertToShort(Object dbzObj, Schema schema) {
        return Short.parseShort(dbzObj.toString());
    }

    protected Object convertToInt(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    protected Object convertToLong(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return ((Integer) dbzObj).longValue();
        } else if (dbzObj instanceof Long) {
            return dbzObj;
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    protected Object convertToDouble(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return ((Float) dbzObj).doubleValue();
        } else if (dbzObj instanceof Double) {
            return dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    protected Object convertToFloat(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double) dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    protected Object convertToDate(Object dbzObj, Schema schema) {
        return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    protected Object convertToTime(Object dbzObj, Schema schema) {
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

    protected Object convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromMillis(micro / 1000, (int) (micro % 1000 * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromMillis(nano / 1000_000, (int) (nano % 1000_000));
            }
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    protected Object convertToLocalTimeZoneTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return LocalZonedTimestampData.fromInstant(instant);
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP_LTZ from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    protected Object convertToString(Object dbzObj, Schema schema) {
        return BinaryStringData.fromString(dbzObj.toString());
    }

    protected Object convertToBinary(Object dbzObj, Schema schema) {
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

    protected Object convertToDecimal(DecimalType decimalType, Object dbzObj, Schema schema) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();

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
                SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            } else {
                // fallback to string
                bigDecimal = new BigDecimal(dbzObj.toString());
            }
        }
        return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
    }

    protected Object convertToRecord(RowType rowType, Object dbzObj, Schema schema)
            throws Exception {
        DeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(DataField::getType)
                        .map(this::createConverter)
                        .toArray(DeserializationRuntimeConverter[]::new);
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        Struct struct = (Struct) dbzObj;
        int arity = fieldNames.length;
        Object[] fields = new Object[arity];
        for (int i = 0; i < arity; i++) {
            String fieldName = fieldNames[i];
            Field field = schema.field(fieldName);
            if (field == null) {
                fields[i] = null;
            } else {
                Object fieldValue = struct.getWithoutDefault(fieldName);
                Schema fieldSchema = schema.field(fieldName).schema();
                Object convertedField = convertField(fieldConverters[i], fieldValue, fieldSchema);
                fields[i] = convertedField;
            }
        }
        return generator.generate(fields);
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
