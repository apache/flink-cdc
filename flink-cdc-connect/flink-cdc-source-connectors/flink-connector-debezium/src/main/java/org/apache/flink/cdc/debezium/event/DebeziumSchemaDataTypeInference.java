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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/** {@link DataType} inference for debezium {@link Schema}. */
@Internal
public class DebeziumSchemaDataTypeInference implements SchemaDataTypeInference, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    public static final int DEFAULT_DECIMAL_PRECISION = 20;

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
        if (Date.SCHEMA_NAME.equals(schema.name())
                || org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.DATE();
        }
        if (Time.SCHEMA_NAME.equals(schema.name())) {
            return DataTypes.TIME(3);
        }
        if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name())) {
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
        if (Timestamp.SCHEMA_NAME.equals(schema.name())
                || org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schema.name())) {
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
                            .map(s -> ZonedTimestamp.FORMATTER.parse(s, Instant::from))
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
            return DataTypes.TIMESTAMP_LTZ(precision);
        }
        return DataTypes.STRING();
    }

    protected DataType inferBytes(Object value, Schema schema) {
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            int scale =
                    Optional.ofNullable(schema.parameters().get(Decimal.SCALE_FIELD))
                            .map(Integer::parseInt)
                            .orElse(DecimalType.DEFAULT_SCALE);

            int precision =
                    Optional.ofNullable(schema.parameters().get(PRECISION_PARAMETER_KEY))
                            .map(Integer::parseInt)
                            .orElse(DEFAULT_DECIMAL_PRECISION);

            if (precision > DecimalType.MAX_PRECISION) {
                return DataTypes.STRING();
            }
            return DataTypes.DECIMAL(precision, scale);
        }
        return DataTypes.BYTES();
    }

    protected DataType inferStruct(Object value, Schema schema) {
        Struct struct = (Struct) value;
        if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            if (struct == null) {
                // set the default value
                return DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
            }
            SpecialValueDecimal decimal = VariableScaleDecimal.toLogical(struct);
            BigDecimal bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            return DataTypes.DECIMAL(bigDecimal.precision(), bigDecimal.scale());
        }
        return DataTypes.ROW(
                schema.fields().stream()
                        .map(
                                f ->
                                        DataTypes.FIELD(
                                                f.name(), infer(struct.get(f.name()), f.schema())))
                        .toArray(DataField[]::new));
    }

    protected DataType inferArray(Object value, Schema schema) {
        Schema elementSchema = schema.valueSchema();
        if (elementSchema != null) {
            DataType elementType = infer(null, elementSchema);
            return DataTypes.ARRAY(elementType);
        } else {
            return DataTypes.ARRAY(DataTypes.STRING());
        }
    }

    protected DataType inferMap(Object value, Schema schema) {
        Schema keySchema = schema.keySchema();
        Schema valueSchema = schema.valueSchema();

        DataType keyType = keySchema != null ? infer(null, keySchema) : DataTypes.STRING();
        DataType valueType = valueSchema != null ? infer(null, valueSchema) : DataTypes.STRING();

        return DataTypes.MAP(keyType, valueType);
    }
}
