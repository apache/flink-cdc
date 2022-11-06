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

package com.ververica.cdc.connectors.oracle.table;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to Oracle. */
public class OracleDeserializationConverterFactory {

    public static DeserializationRuntimeConverterFactory instance() {
        return new DeserializationRuntimeConverterFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<DeserializationRuntimeConverter> createUserDefinedConverter(
                    LogicalType logicalType, ZoneId serverTimeZone) {
                return wrapNumericConverter(createNumericConverter(logicalType, serverTimeZone));
            }
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static Optional<DeserializationRuntimeConverter> createNumericConverter(
            LogicalType type, ZoneId serverTimeZone) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return createBooleanConverter();
            case TINYINT:
                return createByteConverter();
            case SMALLINT:
                return createShortConverter();
            case INTEGER:
                return createIntegerConverter();
            case BIGINT:
                return createLongConverter();
            case FLOAT:
                return createFloatConverter();
            case DOUBLE:
                return createDoubleConverter();
                // Debezium use io.debezium.time.ZonedTimestamp to map Oracle TIMESTAMP WITH LOCAL
                // TIME ZONE type, the value is a string representation of a timestamp in UTC.
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp();
            default:
                // fallback to default converter
                return Optional.empty();
        }
    }

    private static Optional<DeserializationRuntimeConverter> convertToLocalTimeZoneTimestamp() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof String) {
                            String str = (String) dbzObj;
                            // TIMESTAMP_LTZ type is encoded in string type
                            Instant instant = Instant.parse(str);
                            return TimestampData.fromInstant(instant);
                        }
                        throw new IllegalArgumentException(
                                "Unable to convert to TimestampData from unexpected value '"
                                        + dbzObj
                                        + "' of type "
                                        + dbzObj.getClass().getName());
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> wrapNumericConverter(
            Optional<DeserializationRuntimeConverter> converterOptional) {
        return converterOptional.map(
                converter ->
                        new DeserializationRuntimeConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Object dbzObj, Schema schema) throws Exception {
                                if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                                    SpecialValueDecimal decimal =
                                            VariableScaleDecimal.toLogical((Struct) dbzObj);
                                    return converter.convert(
                                            decimal.getDecimalValue().orElse(BigDecimal.ZERO),
                                            schema);
                                }
                                return converter.convert(dbzObj, schema);
                            }
                        });
    }

    private static Optional<DeserializationRuntimeConverter> createBooleanConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Boolean) {
                            return dbzObj;
                        } else if (dbzObj instanceof Byte) {
                            return (byte) dbzObj != 0;
                        } else if (dbzObj instanceof Short) {
                            return (short) dbzObj != 0;
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).shortValue() != 0;
                        } else {
                            return Boolean.parseBoolean(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createByteConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Byte) {
                            return dbzObj;
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).byteValue();
                        } else {
                            return Byte.parseByte(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createShortConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Byte) {
                            return ((Byte) dbzObj).shortValue();
                        } else if (dbzObj instanceof Short) {
                            return dbzObj;
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).shortValue();
                        } else {
                            return Short.parseShort(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createIntegerConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Integer) {
                            return dbzObj;
                        } else if (dbzObj instanceof Long) {
                            return ((Long) dbzObj).intValue();
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).intValue();
                        } else {
                            return Integer.parseInt(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createLongConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Integer) {
                            return ((Integer) dbzObj).longValue();
                        } else if (dbzObj instanceof Long) {
                            return dbzObj;
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).longValue();
                        } else {
                            return Long.parseLong(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createFloatConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Float) {
                            return dbzObj;
                        } else if (dbzObj instanceof Double) {
                            return ((Double) dbzObj).floatValue();
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).floatValue();
                        } else {
                            return Float.parseFloat(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createDoubleConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        if (dbzObj instanceof Float) {
                            return ((Float) dbzObj).doubleValue();
                        } else if (dbzObj instanceof Double) {
                            return dbzObj;
                        } else if (dbzObj instanceof BigDecimal) {
                            return ((BigDecimal) dbzObj).doubleValue();
                        } else {
                            return Double.parseDouble(dbzObj.toString());
                        }
                    }
                });
    }
}
