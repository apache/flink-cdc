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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static com.ververica.cdc.common.types.DecimalType.DEFAULT_PRECISION;

/** {@link DataType} inference for debezium {@link Schema}. */
@Internal
public class DebeziumSchemaDataTypeInference implements SchemaDataTypeInference, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public DataType infer(Object value, Schema schema) {
        return schema.isOptional()
                ? infer(value, schema, schema.type())
                : infer(value, schema, schema.type()).notNull();
    }

    protected DataType infer(Object value, Schema schema, Schema.Type type) {
        switch (type) {
            case INT8:
                return inferInt8(value, schema);
            case INT16:
                return inferInt16(value, schema);
            case INT32:
                return inferInt32(value, schema);
            case INT64:
                return inferInt64(value, schema);
            case FLOAT32:
                return inferFloat32(value, schema);
            case FLOAT64:
                return inferFloat64(value, schema);
            case BOOLEAN:
                return inferBoolean(value, schema);
            case STRING:
                return inferString(value, schema);
            case BYTES:
                return inferBytes(value, schema);
            case STRUCT:
                return inferStruct(value, schema);
            case ARRAY:
                return inferArray(value, schema);
            case MAP:
                return inferMap(value, schema);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + schema.type().getName());
        }
    }

    protected DataType inferBoolean(Object value, Schema schema) {
        return DataTypes.BOOLEAN();
    }

    protected DataType inferInt8(Object value, Schema schema) {
        return DataTypes.TINYINT();
    }

    protected DataType inferInt16(Object value, Schema schema) {
        return DataTypes.SMALLINT();
    }

    protected DataType inferInt32(Object value, Schema schema) {
        if (Date.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.DATE();
        }
        if (Time.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIME(3);
        }
        return DataTypes.INT();
    }

    protected DataType inferInt64(Object value, Schema schema) {
        if (MicroTime.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIME(6);
        }
        if (NanoTime.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIME(9);
        }
        if (Timestamp.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIMESTAMP(3);
        }
        if (MicroTimestamp.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIMESTAMP(6);
        }
        if (NanoTimestamp.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIMESTAMP(9);
        }
        return DataTypes.BIGINT();
    }

    protected DataType inferFloat32(Object value, Schema schema) {
        return DataTypes.FLOAT();
    }

    protected DataType inferFloat64(Object value, Schema schema) {
        return DataTypes.DOUBLE();
    }

    protected DataType inferString(Object value, Schema schema) {
        if (ZonedTimestamp.SCHEMA_NAME.equals(schema.name())) {
            int nano =
                    Optional.ofNullable((String) value)
                            .map(Instant::parse)
                            .map(Instant::getNano)
                            .orElse(0);

            int precision;
            if (nano == 0) {
                precision = 0;
            } else if (nano % 1000 > 0) {
                precision = 9;
            } else if (nano % 1000_000 > 0) {
                precision = 6;
            } else if (nano % 1000_000_000 > 0) {
                precision = 3;
            } else {
                precision = 0;
            }
            return DataTypes.TIMESTAMP(precision);
        }
        return DataTypes.STRING();
    }

    protected DataType inferBytes(Object value, Schema schema) {
        if (Decimal.LOGICAL_NAME.equals(schema.name())
                || VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            if (value instanceof BigDecimal) {
                BigDecimal decimal = (BigDecimal) value;
                return DataTypes.DECIMAL(decimal.precision(), decimal.scale());
            }
            return DataTypes.DECIMAL(DEFAULT_PRECISION, 0);
        }
        return DataTypes.BYTES();
    }

    protected DataType inferStruct(Object value, Schema schema) {
        Struct struct = (Struct) value;
        return DataTypes.ROW(
                schema.fields().stream()
                        .map(
                                f ->
                                        DataTypes.FIELD(
                                                f.name(), infer(struct.get(f.name()), f.schema())))
                        .toArray(DataField[]::new));
    }

    protected DataType inferArray(Object value, Schema schema) {
        throw new UnsupportedOperationException("Unsupported type ARRAY");
    }

    protected DataType inferMap(Object value, Schema schema) {
        throw new UnsupportedOperationException("Unsupported type MAP");
    }
}
