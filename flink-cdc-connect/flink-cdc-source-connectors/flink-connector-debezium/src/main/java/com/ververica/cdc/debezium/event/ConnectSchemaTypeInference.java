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

import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import org.apache.kafka.connect.data.Schema;

/** Utility class to convert {@link Schema} to {@link DataType}. */
public class ConnectSchemaTypeInference {

    public static DataType infer(Schema schema) {
        return schema.isOptional()
                ? infer(schema, schema.type())
                : infer(schema, schema.type()).notNull();
    }

    private static DataType infer(Schema schema, Schema.Type type) {
        switch (type) {
            case INT8:
                return DataTypes.TINYINT();
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case STRING:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case ARRAY:
                return DataTypes.ARRAY(infer(schema.valueSchema()));
            case MAP:
                return DataTypes.MAP(infer(schema.keySchema()), infer(schema.valueSchema()));
            case STRUCT:
                return DataTypes.ROW(
                        schema.fields().stream()
                                .map(f -> DataTypes.FIELD(f.name(), infer(f.schema())))
                                .toArray(DataField[]::new));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + schema.type().getName());
        }
    }
}
