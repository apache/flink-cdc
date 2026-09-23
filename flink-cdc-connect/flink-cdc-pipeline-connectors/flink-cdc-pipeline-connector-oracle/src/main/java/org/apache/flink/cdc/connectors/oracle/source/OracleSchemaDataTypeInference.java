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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geometry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/** {@link DataType} inference for oracle debezium {@link Schema}. */
@Internal
public class OracleSchemaDataTypeInference extends DebeziumSchemaDataTypeInference {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataType inferStruct(Object value, Schema schema) {
        // the Geometry datatype in oracle will be converted to
        // a String with Json format
        if (Geometry.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.STRING();
        } else if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            // For Oracle bare NUMBER and explicit-precision positive-scale
            // NUMBER with p - s > 18, Debezium encodes the value as a
            // VariableScaleDecimal struct. We return DECIMAL(38, 19) to
            // match OracleTypeUtils' schema-level choice (16-byte
            // BinaryRecordData layout is the same for any DECIMAL).
            //
            // For negative-scale NUMBER with (p - s) > 18 (e.g. NUMBER(36, -2)),
            // Debezium also encodes as VariableScaleDecimal but the integer
            // range exceeds BIGINT. OracleTypeUtils returns STRING for these
            // to avoid LONG overflow. We must mirror that STRING choice here
            // at runtime, otherwise the schema/runtime type mismatch would
            // corrupt the BinaryRecordData layout.
            if (value instanceof Struct) {
                Struct struct = (Struct) value;
                Integer dbzScale = struct.getInt32(VariableScaleDecimal.SCALE_FIELD);
                if (dbzScale != null && dbzScale < 0) {
                    return DataTypes.STRING();
                }
            }
            return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 19);
        } else {
            return super.inferStruct(value, schema);
        }
    }
}
