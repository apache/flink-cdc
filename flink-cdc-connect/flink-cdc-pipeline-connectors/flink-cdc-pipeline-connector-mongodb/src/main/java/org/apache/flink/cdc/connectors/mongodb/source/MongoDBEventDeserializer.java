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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.SourceRecordEventDeserializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.internal.HexUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonRegularExpression;
import org.bson.BsonValue;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.apache.flink.cdc.connectors.mongodb.source.SchemaParseMode.SCHEMA_LESS;

/** Event deserializer for {@link MongoDBDataSource}. */
@Internal
public class MongoDBEventDeserializer extends SourceRecordEventDeserializer
        implements DebeziumDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBEventDeserializer.class);

    private final SchemaParseMode schemaParseMode;

    private static final RowType recordIdRowType = MongoDBSchemaUtils.getRecordIdRowType();
    private static final RowType jsonRowType = MongoDBSchemaUtils.getJsonSchemaRowType();

    private static final Map<DataType, DeserializationRuntimeConverter> CONVERTERS =
            new ConcurrentHashMap<>();

    public MongoDBEventDeserializer(SchemaParseMode schemaParseMode) {
        this.schemaParseMode = schemaParseMode;
    }

    protected OperationType operationTypeFor(SourceRecord record) {
        Struct value = (Struct) record.value();
        return OperationType.fromString(value.getString(MongoDBEnvelope.OPERATION_TYPE_FIELD));
    }

    protected BsonDocument extractBsonDocument(Struct value, Schema valueSchema, String fieldName) {
        if (valueSchema.field(fieldName) != null) {
            String docString = value.getString(fieldName);
            if (docString != null) {
                return BsonDocument.parse(docString);
            }
        }
        return null;
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        return true;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return false;
    }

    @Override
    public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) throws Exception {
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        OperationType op = operationTypeFor(record);
        TableId tableId = getTableId(record);
        Map<String, String> meta = getMetadata(record);

        BsonDocument fullDocument =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_FIELD);
        BsonDocument documentKey =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.DOCUMENT_KEY_FIELD);
        switch (op) {
            case INSERT:
                return Collections.singletonList(
                        DataChangeEvent.insertEvent(
                                tableId, extractAfterDataRecord(fullDocument), meta));
            case DELETE:
                return Collections.singletonList(
                        DataChangeEvent.deleteEvent(tableId, extractRecordId(documentKey)));
            case UPDATE:
                return Collections.singletonList(
                        DataChangeEvent.updateEvent(
                                tableId,
                                extractAfterDataRecord(documentKey),
                                extractAfterDataRecord(fullDocument),
                                meta));
            case REPLACE:
            case INVALIDATE:
            case DROP:
            case DROP_DATABASE:
            case RENAME:
            case OTHER:
            default:
                LOG.trace("Received {} operation, skip", op);
                return Collections.emptyList();
        }
    }

    private RecordData extractRecordId(BsonDocument document) throws Exception {
        return (RecordData) getOrCreateConverter(recordIdRowType).convert(document);
    }

    private RecordData extractAfterDataRecord(BsonDocument document) throws Exception {
        RowType type = schemaParseMode == SCHEMA_LESS ? jsonRowType : null;
        return (RecordData) getOrCreateConverter(type).convert(document);
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        return Collections.emptyList();
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        String[] parts = record.topic().split("\\.");
        return TableId.tableId(parts[0], parts[1]);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        return Collections.emptyMap();
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    private DeserializationRuntimeConverter getOrCreateConverter(DataType type) {
        return CONVERTERS.computeIfAbsent(type, this::createConverter);
    }

    private DeserializationRuntimeConverter createConverter(DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    protected static DeserializationRuntimeConverter wrapIntoNullableConverter(
            DeserializationRuntimeConverter converter) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue dbzObj) throws Exception {
                if (dbzObj == null) {
                    return null;
                }
                return converter.convert(dbzObj);
            }
        };
    }

    protected DeserializationRuntimeConverter createNotNullConverter(DataType type) {
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
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final DeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(DataField::getType)
                        .map(this::createConverter)
                        .toArray(DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        return (docObj) -> {
            if (!docObj.isDocument()) {
                throw new IllegalArgumentException(
                        "Unable to convert to rowType from unexpected value '"
                                + docObj
                                + "' of type "
                                + docObj.getBsonType());
            }
            BsonDocument document = docObj.asDocument();
            int arity = fieldNames.length;
            Object[] fields = new Object[arity];

            // Schemaless
            if (rowType.equals(jsonRowType)) {
                fields[0] = convertField(fieldConverters[0], document.get(fieldNames[0]));
                fields[1] = BinaryStringData.fromString(document.toJson());
                return generator.generate(fields);
            }

            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                if (!document.containsKey(fieldName)) {
                    fields[i] = null;
                } else {
                    BsonValue fieldValue = document.get(fieldName);
                    Object convertedField = convertField(fieldConverters[i], fieldValue);
                    fields[i] = convertedField;
                }
            }
            return generator.generate(fields);
        };
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of MongoDB Connect into objects of Flink CDC internal
     * data structures.
     */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(BsonValue docObj) throws Exception;
    }

    private static Object convertField(
            DeserializationRuntimeConverter fieldConverter, BsonValue fieldValue) throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue);
        }
    }

    private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (docObj) -> {
            BigDecimal bigDecimal;
            if (docObj.isString()) {
                bigDecimal = new BigDecimal(docObj.asString().getValue());
            } else if (docObj.isDecimal128()) {
                Decimal128 decimal128Value = docObj.asDecimal128().decimal128Value();
                if (decimal128Value.isFinite()) {
                    bigDecimal = docObj.asDecimal128().decimal128Value().bigDecimalValue();
                } else if (decimal128Value.isNegative()) {
                    bigDecimal = BigDecimal.valueOf(-Double.MAX_VALUE);
                } else {
                    bigDecimal = BigDecimal.valueOf(Double.MAX_VALUE);
                }
            } else if (docObj.isDouble()) {
                bigDecimal = BigDecimal.valueOf(docObj.asDouble().doubleValue());
            } else if (docObj.isInt32()) {
                bigDecimal = BigDecimal.valueOf(docObj.asInt32().getValue());
            } else if (docObj.isInt64()) {
                bigDecimal = BigDecimal.valueOf(docObj.asInt64().getValue());
            } else {
                throw new IllegalArgumentException(
                        "Unable to convert to decimal from unexpected value '"
                                + docObj
                                + "' of type "
                                + docObj.getBsonType());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private Object convertToBinary(BsonValue docObj) {
        if (docObj.isBinary()) {
            return docObj.asBinary().getData();
        }
        throw new UnsupportedOperationException(
                "Unsupported BYTES value type: " + docObj.getClass().getSimpleName());
    }

    private Object convertToString(BsonValue docObj) {
        if (docObj.isString()) {
            return BinaryStringData.fromString(docObj.asString().getValue());
        }
        if (docObj.isDocument()) {
            // convert document to json string
            return BinaryStringData.fromString(docObj.asDocument().toJson());
        }
        if (docObj.isBinary()) {
            BsonBinary bsonBinary = docObj.asBinary();
            if (BsonBinarySubType.isUuid(bsonBinary.getType())) {
                return BinaryStringData.fromString(bsonBinary.asUuid().toString());
            }
            return BinaryStringData.fromString(HexUtils.toHex(bsonBinary.getData()));
        }
        if (docObj.isObjectId()) {
            return BinaryStringData.fromString(docObj.asObjectId().getValue().toHexString());
        }
        if (docObj.isInt32()) {
            return BinaryStringData.fromString(String.valueOf(docObj.asInt32().getValue()));
        }
        if (docObj.isInt64()) {
            return BinaryStringData.fromString(String.valueOf(docObj.asInt64().getValue()));
        }
        if (docObj.isDouble()) {
            return BinaryStringData.fromString(String.valueOf(docObj.asDouble().getValue()));
        }
        if (docObj.isDecimal128()) {
            return BinaryStringData.fromString(docObj.asDecimal128().getValue().toString());
        }
        if (docObj.isBoolean()) {
            return BinaryStringData.fromString(String.valueOf(docObj.asBoolean().getValue()));
        }
        if (docObj.isDateTime()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return BinaryStringData.fromString(
                    convertInstantToLocalDateTime(instant).format(ISO_OFFSET_DATE_TIME));
        }
        if (docObj.isTimestamp()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return BinaryStringData.fromString(
                    convertInstantToLocalDateTime(instant).format(ISO_OFFSET_DATE_TIME));
        }
        if (docObj.isArray()) {
            // convert bson array to json string
            Writer writer = new StringWriter();
            JsonWriter jsonArrayWriter =
                    new JsonWriter(writer) {
                        @Override
                        public void writeStartArray() {
                            doWriteStartArray();
                            setState(State.VALUE);
                        }

                        @Override
                        public void writeEndArray() {
                            doWriteEndArray();
                            setState(getNextState());
                        }
                    };

            new BsonArrayCodec()
                    .encode(jsonArrayWriter, docObj.asArray(), EncoderContext.builder().build());
            return BinaryStringData.fromString(writer.toString());
        }
        if (docObj.isRegularExpression()) {
            BsonRegularExpression regex = docObj.asRegularExpression();
            return BinaryStringData.fromString(
                    String.format("/%s/%s", regex.getPattern(), regex.getOptions()));
        }
        if (docObj.isJavaScript()) {
            return BinaryStringData.fromString(docObj.asJavaScript().getCode());
        }
        if (docObj.isJavaScriptWithScope()) {
            return BinaryStringData.fromString(docObj.asJavaScriptWithScope().getCode());
        }
        if (docObj.isSymbol()) {
            return BinaryStringData.fromString(docObj.asSymbol().getSymbol());
        }
        if (docObj.isDBPointer()) {
            return BinaryStringData.fromString(docObj.asDBPointer().getId().toHexString());
        }
        if (docObj instanceof BsonMinKey || docObj instanceof BsonMaxKey) {
            return BinaryStringData.fromString(docObj.getBsonType().name());
        }
        throw new IllegalArgumentException(
                "Unable to convert to string from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToDouble(BsonValue docObj) {
        if (docObj.isNumber()) {
            return docObj.asNumber().doubleValue();
        }
        if (docObj.isDecimal128()) {
            Decimal128 decimal128Value = docObj.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.doubleValue();
            } else if (decimal128Value.isNegative()) {
                return -Double.MAX_VALUE;
            } else {
                return Double.MAX_VALUE;
            }
        }
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? 1 : 0;
        }
        if (docObj.isString()) {
            return Double.parseDouble(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to double from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToFloat(BsonValue docObj) {
        if (docObj.isInt32()) {
            return docObj.asInt32().getValue();
        }
        if (docObj.isInt64()) {
            return docObj.asInt64().getValue();
        }
        if (docObj.isDouble()) {
            return ((Double) docObj.asDouble().getValue()).floatValue();
        }
        if (docObj.isDecimal128()) {
            Decimal128 decimal128Value = docObj.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.floatValue();
            } else if (decimal128Value.isNegative()) {
                return -Float.MAX_VALUE;
            } else {
                return Float.MAX_VALUE;
            }
        }
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? 1f : 0f;
        }
        if (docObj.isString()) {
            return Float.parseFloat(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to float from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToLocalTimeZoneTimestamp(BsonValue docObj) {
        if (docObj.isDateTime()) {
            return LocalZonedTimestampData.fromEpochMillis(docObj.asDateTime().getValue());
        }
        if (docObj.isTimestamp()) {
            return LocalZonedTimestampData.fromEpochMillis(docObj.asTimestamp().getTime() * 1000L);
        }
        throw new IllegalArgumentException(
                "Unable to convert to timestamp with local timezone from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToTimestamp(BsonValue docObj) {
        if (docObj.isDateTime()) {
            return TimestampData.fromLocalDateTime(
                    convertInstantToLocalDateTime(convertToInstant(docObj.asDateTime())));
        }
        if (docObj.isTimestamp()) {
            return TimestampData.fromLocalDateTime(
                    convertInstantToLocalDateTime(convertToInstant(docObj.asDateTime())));
        }

        throw new IllegalArgumentException(
                "Unable to convert to timestamp from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToTime(BsonValue docObj) {
        if (docObj.isDateTime()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return convertInstantToLocalTime(instant).toSecondOfDay() * 1000;
        }
        if (docObj.isTimestamp()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return convertInstantToLocalTime(instant).toSecondOfDay() * 1000;
        }
        throw new IllegalArgumentException(
                "Unable to convert to time from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private LocalTime convertInstantToLocalTime(Instant instant) {
        return convertInstantToLocalDateTime(instant).toLocalTime();
    }

    private Object convertToDate(BsonValue docObj) {
        if (docObj.isDateTime()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return (int) convertInstantToLocalDate(instant).toEpochDay();
        }
        if (docObj.isTimestamp()) {
            Instant instant = convertToInstant(docObj.asDateTime());
            return (int) convertInstantToLocalDate(instant).toEpochDay();
        }
        throw new IllegalArgumentException(
                "Unable to convert to date from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Instant convertToInstant(BsonDateTime bsonDateTime) {
        return Instant.ofEpochMilli(bsonDateTime.getValue());
    }

    private LocalDate convertInstantToLocalDate(Instant instant) {
        return convertInstantToLocalDateTime(instant).toLocalDate();
    }

    private LocalDateTime convertInstantToLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    }

    private Object convertToLong(BsonValue docObj) {
        if (docObj.isNumber()) {
            return docObj.asNumber().longValue();
        }
        if (docObj.isDecimal128()) {
            Decimal128 decimal128Value = docObj.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.longValue();
            } else if (decimal128Value.isNegative()) {
                return Long.MIN_VALUE;
            } else {
                return Long.MAX_VALUE;
            }
        }
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? 1L : 0L;
        }
        if (docObj.isDateTime()) {
            return docObj.asDateTime().getValue();
        }
        if (docObj.isTimestamp()) {
            return docObj.asTimestamp().getTime() * 1000L;
        }
        if (docObj.isString()) {
            return Long.parseLong(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to long from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToInt(BsonValue docObj) {
        if (docObj.isNumber()) {
            return docObj.asNumber().intValue();
        }
        if (docObj.isDecimal128()) {
            Decimal128 decimal128Value = docObj.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.intValue();
            } else if (decimal128Value.isNegative()) {
                return Integer.MIN_VALUE;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? 1 : 0;
        }
        if (docObj.isDateTime()) {
            return Math.toIntExact(docObj.asDateTime().getValue() / 1000L);
        }
        if (docObj.isTimestamp()) {
            return docObj.asTimestamp().getTime();
        }
        if (docObj.isString()) {
            return Integer.parseInt(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to integer from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToShort(BsonValue docObj) {
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? (short) 1 : (short) 0;
        }
        if (docObj.isInt32()) {
            return (short) docObj.asInt32().getValue();
        }
        if (docObj.isInt64()) {
            return (short) docObj.asInt64().getValue();
        }
        if (docObj.isString()) {
            return Short.parseShort(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to smallint from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToByte(BsonValue docObj) {
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue() ? (byte) 1 : (byte) 0;
        }
        if (docObj.isInt32()) {
            return (byte) docObj.asInt32().getValue();
        }
        if (docObj.isInt64()) {
            return (byte) docObj.asInt64().getValue();
        }
        if (docObj.isString()) {
            return Byte.parseByte(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to tinyint from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    private Object convertToBoolean(BsonValue docObj) {
        if (docObj.isBoolean()) {
            return docObj.asBoolean().getValue();
        }
        if (docObj.isInt32()) {
            return docObj.asInt32().getValue() == 1;
        }
        if (docObj.isInt64()) {
            return docObj.asInt64().getValue() == 1L;
        }
        if (docObj.isString()) {
            return Boolean.parseBoolean(docObj.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to boolean from unexpected value '"
                        + docObj
                        + "' of type "
                        + docObj.getBsonType());
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        deserialize(record).forEach(out::collect);
    }
}
