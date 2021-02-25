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

package com.alibaba.ververica.cdc.debezium.table;

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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link RowData}.
 */
public final class RowDataDebeziumDeserializeSchema implements DebeziumDeserializationSchema<RowData> {
	private static final long serialVersionUID = -4852684966051743776L;

	/**
	 * Custom validator to validate the row value.
	 */
	public interface ValueValidator extends Serializable {
		default boolean filter(SourceRecord sourceRecord){
			return false;
		}

		void validate(RowData rowData, RowKind rowKind) throws Exception;
	}

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Runtime converter that converts {@link JsonNode}s into
	 * objects of Flink SQL internal data structures. **/
	private final DeserializationRuntimeConverter runtimeConverter;

	/**
	 * Time zone of the database server.
	 */
	private final ZoneId serverTimeZone;

	/**
	 * Validator to validate the row value.
	 */
	private final ValueValidator validator;

	public RowDataDebeziumDeserializeSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, ValueValidator validator, ZoneId serverTimeZone) {
		this.runtimeConverter = createConverter(rowType);
		this.resultTypeInfo = resultTypeInfo;
		this.validator = validator;
		this.serverTimeZone = serverTimeZone;
	}

	@Override
	public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
		if (validator.filter(record)){
			return;
		}
		Envelope.Operation op = Envelope.operationFor(record);
		Struct value = (Struct) record.value();
		Schema valueSchema = record.valueSchema();
		if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
			GenericRowData insert = extractAfterRow(value, valueSchema);
			validator.validate(insert, RowKind.INSERT);
			insert.setRowKind(RowKind.INSERT);
			out.collect(insert);
		} else if (op == Envelope.Operation.DELETE) {
			GenericRowData delete = extractBeforeRow(value, valueSchema);
			validator.validate(delete, RowKind.DELETE);
			delete.setRowKind(RowKind.DELETE);
			out.collect(delete);
		} else {
			GenericRowData before = extractBeforeRow(value, valueSchema);
			validator.validate(before, RowKind.UPDATE_BEFORE);
			before.setRowKind(RowKind.UPDATE_BEFORE);
			out.collect(before);

			GenericRowData after = extractAfterRow(value, valueSchema);
			validator.validate(after, RowKind.UPDATE_AFTER);
			after.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(after);
		}
	}

	private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
		Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
		Struct after = value.getStruct(Envelope.FieldName.AFTER);
		return (GenericRowData) runtimeConverter.convert(after, afterSchema);
	}

	private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
		Schema afterSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
		Struct after = value.getStruct(Envelope.FieldName.BEFORE);
		return (GenericRowData) runtimeConverter.convert(after, afterSchema);
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Debezium into objects of Flink Table & SQL internal data structures.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(Object dbzObj, Schema schema) throws Exception;
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return (dbzObj, schema) -> null;
			case BOOLEAN:
				return this::convertToBoolean;
			case TINYINT:
				return (dbzObj, schema) -> Byte.parseByte(dbzObj.toString());
			case SMALLINT:
				return (dbzObj, schema) -> Short.parseShort(dbzObj.toString());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return this::convertToInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
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
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Boolean) {
			return (boolean) dbzObj;
		} else if (dbzObj instanceof Byte) {
			return (byte) dbzObj == 1;
		} else if (dbzObj instanceof Short) {
			return (short) dbzObj == 1;
		} else {
			return Boolean.parseBoolean(dbzObj.toString());
		}
	}

	private int convertToInt(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Integer) {
			return (int) dbzObj;
		} else if (dbzObj instanceof Long) {
			return ((Long) dbzObj).intValue();
		} else {
			return Integer.parseInt(dbzObj.toString());
		}
	}

	private long convertToLong(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Integer) {
			return (long) dbzObj;
		} else if (dbzObj instanceof Long) {
			return (long) dbzObj;
		} else {
			return Long.parseLong(dbzObj.toString());
		}
	}

	private double convertToDouble(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Float) {
			return (double) dbzObj;
		} else if (dbzObj instanceof Double) {
			return (double) dbzObj;
		} else {
			return Double.parseDouble(dbzObj.toString());
		}
	}

	private float convertToFloat(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Float) {
			return (float) dbzObj;
		} else if (dbzObj instanceof Double) {
			return ((Double) dbzObj).floatValue();
		} else {
			return Float.parseFloat(dbzObj.toString());
		}
	}

	private int convertToDate(Object dbzObj, Schema schema) {
		return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
	}

	private int convertToTime(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Long) {
			switch (schema.name()) {
				case MicroTime.SCHEMA_NAME:
					return (int) ((long) dbzObj / 1000);
				case NanoTime.SCHEMA_NAME:
					return (int) ((long) dbzObj / 1000_000);
			}
		} else if (dbzObj instanceof Integer) {
			return (int) dbzObj;
		}
		// get number of milliseconds of the day
		return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(Object dbzObj, Schema schema) {
		if (dbzObj instanceof Long) {
			switch (schema.name()) {
				case Timestamp.SCHEMA_NAME:
					return TimestampData.fromEpochMillis((Long) dbzObj);
				case MicroTimestamp.SCHEMA_NAME:
					long micro = (long) dbzObj;
					return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000));
				case NanoTimestamp.SCHEMA_NAME:
					long nano = (long) dbzObj;
					return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000));
			}
		}
		LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
		return TimestampData.fromLocalDateTime(localDateTime);
	}

	private TimestampData convertToLocalTimeZoneTimestamp(Object dbzObj, Schema schema) {
		if (dbzObj instanceof String) {
			String str = (String) dbzObj;
			// TIMESTAMP type is encoded in string type
			Instant instant = Instant.parse(str);
			return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone));
		}
		throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value '" + dbzObj + "' of type " + dbzObj.getClass().getName());
	}

	private StringData convertToString(Object dbzObj, Schema schema) {
		return StringData.fromString(dbzObj.toString());
	}

	private byte[] convertToBinary(Object dbzObj, Schema schema) {
		if (dbzObj instanceof byte[]) {
			return (byte[]) dbzObj;
		} else if (dbzObj instanceof ByteBuffer) {
			ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
			byte[] bytes = new byte[byteBuffer.remaining()];
			byteBuffer.get(bytes);
			return bytes;
		} else {
			throw new UnsupportedOperationException("Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
		}
	}

	private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (dbzObj, schema) -> {
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
		};
	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return (dbzObj, schema) -> {
			Struct struct = (Struct) dbzObj;
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				Object fieldValue = struct.get(fieldName);
				Schema fieldSchema = schema.field(fieldName).schema();
				Object convertedField = convertField(fieldConverters[i], fieldValue, fieldSchema);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private Object convertField(
			DeserializationRuntimeConverter fieldConverter,
			Object fieldValue,
			Schema fieldSchema) throws Exception {
		if (fieldValue == null) {
			return null;
		} else {
			return fieldConverter.convert(fieldValue, fieldSchema);
		}
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
		DeserializationRuntimeConverter converter) {
		return (dbzObj, schema) -> {
			if (dbzObj == null) {
				return null;
			}
			return converter.convert(dbzObj, schema);
		};
	}
}
