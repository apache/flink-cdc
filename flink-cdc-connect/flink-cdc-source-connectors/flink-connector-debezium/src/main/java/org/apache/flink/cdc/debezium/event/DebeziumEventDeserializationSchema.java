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

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private final Map<io.debezium.relational.TableId, CreateTableEvent> createTableEventCache;

    public DebeziumEventDeserializationSchema(
            SchemaDataTypeInference schemaDataTypeInference, DebeziumChangelogMode changelogMode) {
        this.schemaDataTypeInference = schemaDataTypeInference;
        this.changelogMode = changelogMode;
        this.createTableEventCache = new HashMap<>();
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        deserialize(record).forEach(out::collect);
    }

    @Override
    public List<? extends Event> deserialize(SourceRecord record) throws Exception {
        List<? extends Event> events = super.deserialize(record);
        for (Event event : events) {
            if (event instanceof SchemaChangeEvent) {
                applyChangeEvent((SchemaChangeEvent) event);
            }
        }
        return events;
    }

    @Override
    public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        TableId tableId = getTableId(record);

        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Map<String, String> meta = getMetadata(record);

        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            RecordData after = extractAfterDataRecord(tableId, value, valueSchema);
            return Collections.singletonList(DataChangeEvent.insertEvent(tableId, after, meta));
        } else if (op == Envelope.Operation.DELETE) {
            RecordData before = extractBeforeDataRecord(tableId, value, valueSchema);
            return Collections.singletonList(DataChangeEvent.deleteEvent(tableId, before, meta));
        } else if (op == Envelope.Operation.UPDATE) {
            RecordData after = extractAfterDataRecord(tableId, value, valueSchema);
            if (changelogMode == DebeziumChangelogMode.ALL) {
                RecordData before = extractBeforeDataRecord(tableId, value, valueSchema);
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

    private RecordData extractBeforeDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        Schema beforeSchema = fieldSchema(valueSchema, Envelope.FieldName.BEFORE);
        Struct beforeValue = fieldStruct(value, Envelope.FieldName.BEFORE);
        return extractDataRecord(tableId, beforeValue, beforeSchema);
    }

    private RecordData extractAfterDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        Schema afterSchema = fieldSchema(valueSchema, Envelope.FieldName.AFTER);
        Struct afterValue = fieldStruct(value, Envelope.FieldName.AFTER);
        return extractDataRecord(tableId, afterValue, afterSchema);
    }

    private RecordData extractDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        if (value == null) {
            return null;
        }

        CreateTableEvent createTableEvent =
                getCreateTableEventCache().get(toDebeziumTableId(tableId));
        if (createTableEvent != null) {
            try {
                DataType dataType = createTableEvent.getSchema().toRowDataType();
                return (RecordData) getOrCreateConverter(dataType).convert(value, valueSchema);
            } catch (CachedSchemaMismatchException e) {
                LOG.info(
                        "Cached schema for {} is incompatible with the current record schema, falling back to value inference: {}",
                        tableId,
                        e.getMessage());
            }
        }

        DataType dataType = schemaDataTypeInference.infer(value, valueSchema);
        return (RecordData) getOrCreateConverter(dataType).convert(value, valueSchema);
    }

    private io.debezium.relational.TableId toDebeziumTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getNamespace(), tableId.getSchemaName(), tableId.getTableName());
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
            case MAP:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        return convertToMap(dbzObj, schema);
                    }
                };
            case ARRAY:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        return convertToArray(dbzObj, schema);
                    }
                };
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
        if (dbzObj instanceof Date) {
            Instant instant = ((Date) dbzObj).toInstant();
            return DateData.fromLocalDate(instant.atZone(java.time.ZoneOffset.UTC).toLocalDate());
        }
        return DateData.fromLocalDate(TemporalConversions.toLocalDate(dbzObj));
    }

    protected Object convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case MicroTime.SCHEMA_NAME:
                    return TimeData.fromMicroOfDay((long) dbzObj);
                case NanoTime.SCHEMA_NAME:
                    return TimeData.fromNanoOfDay((long) dbzObj);
            }
        } else if (dbzObj instanceof Integer) {
            return TimeData.fromMillisOfDay((int) dbzObj);
        } else if (dbzObj instanceof Date) {
            long millisOfDay = ((Date) dbzObj).getTime() % (24 * 60 * 60 * 1000);
            return TimeData.fromMillisOfDay((int) millisOfDay);
        }
        // get number of milliseconds of the day
        return TimeData.fromLocalTime(TemporalConversions.toLocalTime(dbzObj));
    }

    protected Object convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromMillis(
                            Math.floorDiv(micro, 1000), (int) (Math.floorMod(micro, 1000) * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromMillis(
                            Math.floorDiv(nano, 1000_000), (int) (Math.floorMod(nano, 1000_000)));
            }
        }
        if (dbzObj instanceof Date) {
            if (schema.name().equals(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME)) {
                Instant instant = ((Date) dbzObj).toInstant();
                return TimestampData.fromMillis(instant.toEpochMilli());
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
            Instant instant = ZonedTimestamp.FORMATTER.parse(str, Instant::from);
            return LocalZonedTimestampData.fromInstant(instant);
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP_LTZ from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    protected Object convertToString(Object dbzObj, Schema schema) {
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            if (dbzObj instanceof BigDecimal) {
                return BinaryStringData.fromString(((BigDecimal) dbzObj).toPlainString());
            }
            if (dbzObj instanceof byte[]) {
                return BinaryStringData.fromString(
                        Decimal.toLogical(schema, (byte[]) dbzObj).toPlainString());
            }
            if (dbzObj instanceof ByteBuffer) {
                ByteBuffer byteBuffer = ((ByteBuffer) dbzObj).duplicate();
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                return BinaryStringData.fromString(
                        Decimal.toLogical(schema, bytes).toPlainString());
            }
        }
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
        if (!(dbzObj instanceof Struct)) {
            throw new CachedSchemaMismatchException(
                    "Expected STRUCT payload but got "
                            + dbzObj.getClass().getName()
                            + " for cached row type "
                            + rowType);
        }
        if (schema.type() != Schema.Type.STRUCT) {
            throw new CachedSchemaMismatchException(
                    "Expected STRUCT schema but got "
                            + schema.type()
                            + " for cached row type "
                            + rowType);
        }
        String unexpectedField = findUnexpectedActualField(null, rowType, schema);
        if (unexpectedField != null) {
            throw new CachedSchemaMismatchException(unexpectedField);
        }
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
                throw new CachedSchemaMismatchException(
                        "Missing field '" + fieldName + "' in current record schema.");
            }
            Object fieldValue = struct.getWithoutDefault(fieldName);
            Schema fieldSchema = field.schema();
            String incompatibility =
                    findCachedFieldIncompatibility(fieldName, rowType.getTypeAt(i), fieldSchema);
            if (incompatibility != null) {
                throw new CachedSchemaMismatchException(incompatibility);
            }
            Object convertedField = convertField(fieldConverters[i], fieldValue, fieldSchema);
            fields[i] = convertedField;
        }
        return generator.generate(fields);
    }

    private String findCachedFieldIncompatibility(
            String fieldPath, DataType expectedType, Schema actualSchema) {
        switch (expectedType.getTypeRoot()) {
            case BOOLEAN:
                return isBooleanSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case TINYINT:
                return actualSchema.type() == Schema.Type.INT8
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case SMALLINT:
                return actualSchema.type() == Schema.Type.INT16
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case INTEGER:
                return isPlainSchemaCompatible(actualSchema, Schema.Type.INT32)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case BIGINT:
                return isPlainSchemaCompatible(actualSchema, Schema.Type.INT64)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case FLOAT:
                return actualSchema.type() == Schema.Type.FLOAT32
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case DOUBLE:
                return actualSchema.type() == Schema.Type.FLOAT64
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case CHAR:
            case VARCHAR:
                return isStringSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case BINARY:
            case VARBINARY:
                return isPlainSchemaCompatible(actualSchema, Schema.Type.BYTES)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case DECIMAL:
                return isDecimalSchemaCompatible((DecimalType) expectedType, actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case DATE:
                return isDateSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case TIME_WITHOUT_TIME_ZONE:
                return isTimeSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return isTimestampSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return isLocalTimeZoneTimestampSchemaCompatible(actualSchema)
                        ? null
                        : formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
            case ARRAY:
                if (actualSchema.type() != Schema.Type.ARRAY) {
                    return formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
                }
                return findCachedFieldIncompatibility(
                        fieldPath + "[]",
                        expectedType.getChildren().get(0),
                        actualSchema.valueSchema());
            case MAP:
                if (actualSchema.type() != Schema.Type.MAP) {
                    return formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
                }
                String keyMismatch =
                        findCachedFieldIncompatibility(
                                fieldPath + "<key>",
                                expectedType.getChildren().get(0),
                                actualSchema.keySchema());
                if (keyMismatch != null) {
                    return keyMismatch;
                }
                return findCachedFieldIncompatibility(
                        fieldPath + "<value>",
                        expectedType.getChildren().get(1),
                        actualSchema.valueSchema());
            case ROW:
                if (actualSchema.type() != Schema.Type.STRUCT) {
                    return formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
                }
                RowType expectedRowType = (RowType) expectedType;
                String unexpectedField =
                        findUnexpectedActualField(fieldPath, expectedRowType, actualSchema);
                if (unexpectedField != null) {
                    return unexpectedField;
                }
                for (DataField nestedField : expectedRowType.getFields()) {
                    Field actualField = actualSchema.field(nestedField.getName());
                    if (actualField == null) {
                        return "Missing field '"
                                + fieldPath
                                + "."
                                + nestedField.getName()
                                + "' in current record schema.";
                    }
                    String nestedMismatch =
                            findCachedFieldIncompatibility(
                                    fieldPath + "." + nestedField.getName(),
                                    nestedField.getType(),
                                    actualField.schema());
                    if (nestedMismatch != null) {
                        return nestedMismatch;
                    }
                }
                return null;
            default:
                return formatCachedFieldMismatch(fieldPath, expectedType, actualSchema);
        }
    }

    private String findUnexpectedActualField(
            String fieldPath, RowType expectedRowType, Schema actualSchema) {
        List<String> expectedFieldNames = expectedRowType.getFieldNames();
        for (Field actualField : actualSchema.fields()) {
            if (!expectedFieldNames.contains(actualField.name())) {
                return "Unexpected field '"
                        + qualifyFieldPath(fieldPath, actualField.name())
                        + "' in current record schema.";
            }
        }
        return null;
    }

    private static String qualifyFieldPath(String fieldPath, String fieldName) {
        if (fieldPath == null || fieldPath.isEmpty()) {
            return fieldName;
        }
        return fieldPath + "." + fieldName;
    }

    private static boolean isBooleanSchemaCompatible(Schema schema) {
        return schema.type() == Schema.Type.BOOLEAN
                || schema.type() == Schema.Type.INT8
                || schema.type() == Schema.Type.INT16;
    }

    private static boolean isPlainSchemaCompatible(Schema schema, Schema.Type expectedType) {
        return schema.type() == expectedType && schema.name() == null;
    }

    private static boolean isStringSchemaCompatible(Schema schema) {
        if (schema.type() == Schema.Type.STRING) {
            return !ZonedTimestamp.SCHEMA_NAME.equals(schema.name());
        }
        // Large-precision DECIMAL columns may be modeled as STRING in Flink CDC schema while
        // Debezium still exposes them as Kafka Connect Decimal logical BYTES at runtime.
        if (schema.type() == Schema.Type.BYTES && Decimal.LOGICAL_NAME.equals(schema.name())) {
            Map<String, String> parameters = schema.parameters();
            int scale =
                    Optional.ofNullable(parameters)
                            .map(p -> p.get(Decimal.SCALE_FIELD))
                            .map(Integer::parseInt)
                            .orElse(DecimalType.DEFAULT_SCALE);
            int precision =
                    Optional.ofNullable(parameters)
                            .map(
                                    p ->
                                            p.get(
                                                    DebeziumSchemaDataTypeInference
                                                            .PRECISION_PARAMETER_KEY))
                            .map(Integer::parseInt)
                            .orElse(DebeziumSchemaDataTypeInference.DEFAULT_DECIMAL_PRECISION);
            return precision > DecimalType.MAX_PRECISION || scale < 0 || scale > 36;
        }
        // Pipeline connectors may model geometry logical STRUCT payloads as STRING and render them
        // to connector-specific JSON in their custom convertToString implementations.
        return schema.type() == Schema.Type.STRUCT
                && (Point.LOGICAL_NAME.equals(schema.name())
                        || Geometry.LOGICAL_NAME.equals(schema.name())
                        || Geography.LOGICAL_NAME.equals(schema.name()));
    }

    private static boolean isDecimalSchemaCompatible(DecimalType expectedType, Schema schema) {
        if (schema.type() == Schema.Type.STRING || schema.type() == Schema.Type.FLOAT64) {
            return true;
        }
        if (schema.type() == Schema.Type.STRUCT
                && VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            return true;
        }
        if (schema.type() != Schema.Type.BYTES || !Decimal.LOGICAL_NAME.equals(schema.name())) {
            return false;
        }
        Map<String, String> parameters = schema.parameters();
        int scale =
                Optional.ofNullable(parameters)
                        .map(p -> p.get(Decimal.SCALE_FIELD))
                        .map(Integer::parseInt)
                        .orElse(DecimalType.DEFAULT_SCALE);
        if (expectedType.getScale() != scale) {
            return false;
        }
        String precision =
                Optional.ofNullable(parameters)
                        .map(p -> p.get(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY))
                        .orElse(null);
        return precision == null || expectedType.getPrecision() == Integer.parseInt(precision);
    }

    private static boolean isDateSchemaCompatible(Schema schema) {
        return schema.type() == Schema.Type.INT32
                && (io.debezium.time.Date.SCHEMA_NAME.equals(schema.name())
                        || org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name()));
    }

    private static boolean isTimeSchemaCompatible(Schema schema) {
        if (schema.type() == Schema.Type.INT32) {
            return io.debezium.time.Time.SCHEMA_NAME.equals(schema.name())
                    || org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name());
        }
        if (schema.type() == Schema.Type.INT64) {
            return MicroTime.SCHEMA_NAME.equals(schema.name())
                    || NanoTime.SCHEMA_NAME.equals(schema.name());
        }
        return false;
    }

    private static boolean isTimestampSchemaCompatible(Schema schema) {
        if (schema.type() != Schema.Type.INT64) {
            return false;
        }
        return Timestamp.SCHEMA_NAME.equals(schema.name())
                || MicroTimestamp.SCHEMA_NAME.equals(schema.name())
                || NanoTimestamp.SCHEMA_NAME.equals(schema.name())
                || org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schema.name());
    }

    private static boolean isLocalTimeZoneTimestampSchemaCompatible(Schema schema) {
        return schema.type() == Schema.Type.STRING
                && ZonedTimestamp.SCHEMA_NAME.equals(schema.name());
    }

    private static String formatCachedFieldMismatch(
            String fieldPath, DataType expectedType, Schema actualSchema) {
        return "Field '"
                + fieldPath
                + "' expects cached type "
                + expectedType
                + " but current record schema is "
                + describeSchema(actualSchema)
                + ".";
    }

    private static String describeSchema(Schema schema) {
        return schema.name() == null
                ? schema.type().name()
                : schema.type().name() + "/" + schema.name();
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

    protected Object convertToMap(Object dbzObj, Schema schema) throws Exception {
        if (dbzObj == null) {
            return null;
        }

        // Obtain the schema for the keys and values of a Map"
        Schema keySchema = schema.keySchema();
        Schema valueSchema = schema.valueSchema();

        // Infer the data types of keys and values
        DataType keyType =
                keySchema != null
                        ? schemaDataTypeInference.infer(null, keySchema)
                        : DataTypes.STRING();

        DataType valueType =
                valueSchema != null
                        ? schemaDataTypeInference.infer(null, valueSchema)
                        : DataTypes.STRING();

        DeserializationRuntimeConverter keyConverter = createConverter(keyType);
        DeserializationRuntimeConverter valueConverter = createConverter(valueType);

        Map<?, ?> map = (Map<?, ?>) dbzObj;
        Map<Object, Object> convertedMap = new java.util.HashMap<>(map.size());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object convertedKey = convertField(keyConverter, entry.getKey(), keySchema);
            Object convertedValue = convertField(valueConverter, entry.getValue(), valueSchema);
            convertedMap.put(convertedKey, convertedValue);
        }

        return new GenericMapData(convertedMap);
    }

    protected Object convertToArray(Object dbzObj, Schema schema) throws Exception {
        if (dbzObj == null) {
            return null;
        }

        Schema elementSchema = schema.valueSchema();
        // Multidimensional arrays are not supported
        if (elementSchema.type() == Schema.Type.ARRAY) {
            throw new IllegalArgumentException(
                    "Unable to convert multidimensional array value '"
                            + dbzObj
                            + "' to a flat array.");
        }
        DataType elementType = schemaDataTypeInference.infer(null, elementSchema);
        DeserializationRuntimeConverter elementConverter = getOrCreateConverter(elementType);

        if (dbzObj instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) dbzObj;
            Object[] array = new Object[list.size()];

            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                if (element == null) {
                    array[i] = null;
                } else {
                    array[i] = elementConverter.convert(element, elementSchema);
                }
            }

            return new GenericArrayData(array);
        } else if (dbzObj instanceof Object[]) {
            Object[] inputArray = (Object[]) dbzObj;
            Object[] convertedArray = new Object[inputArray.length];

            for (int i = 0; i < inputArray.length; i++) {
                if (inputArray[i] == null) {
                    convertedArray[i] = null;
                } else {
                    convertedArray[i] = elementConverter.convert(inputArray[i], elementSchema);
                }
            }

            return new GenericArrayData(convertedArray);
        }

        throw new IllegalArgumentException(
                "Unable to convert to Array from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
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

    public Map<io.debezium.relational.TableId, CreateTableEvent> getCreateTableEventCache() {
        return createTableEventCache;
    }

    public void applyChangeEvent(ChangeEvent changeEvent) {
        if (!(changeEvent instanceof SchemaChangeEvent)) {
            return;
        }
        org.apache.flink.cdc.common.event.TableId flinkTableId = changeEvent.tableId();

        io.debezium.relational.TableId debeziumTableId =
                new io.debezium.relational.TableId(
                        flinkTableId.getNamespace(),
                        flinkTableId.getSchemaName(),
                        flinkTableId.getTableName());

        if (changeEvent instanceof DropTableEvent) {
            createTableEventCache.remove(debeziumTableId);
            return;
        }

        if (changeEvent instanceof CreateTableEvent) {
            createTableEventCache.put(debeziumTableId, (CreateTableEvent) changeEvent);
            return;
        }

        CreateTableEvent existing = createTableEventCache.get(debeziumTableId);
        if (existing == null) {
            return;
        }

        org.apache.flink.cdc.common.schema.Schema updatedSchema =
                SchemaUtils.applySchemaChangeEvent(
                        existing.getSchema(), (SchemaChangeEvent) changeEvent);
        createTableEventCache.put(
                debeziumTableId, new CreateTableEvent(existing.tableId(), updatedSchema));
    }

    private static final class CachedSchemaMismatchException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private CachedSchemaMismatchException(String message) {
            super(message);
        }
    }
}
