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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

/** Parses a Debezium / Kafka Connect field schema into a CDC {@link Schema}. */
class DebeziumJsonSchemaParser {

    Schema parseSchema(JsonNode rowSchema) {
        Schema.Builder builder = Schema.newBuilder();
        for (JsonNode field : rowSchema.path("fields")) {
            builder.physicalColumn(
                    requiredText(field, "field", "Debezium row schema field"), parseType(field));
        }
        return builder.build();
    }

    JsonNode findFieldSchema(JsonNode envelopeSchema, String fieldName) {
        for (JsonNode field : envelopeSchema.path("fields")) {
            if (fieldName.equals(field.path("field").asText())) {
                return field;
            }
        }
        return null;
    }

    private DataType parseType(JsonNode schema) {
        String logicalName = schema.path("name").asText("");
        DataType type;
        switch (logicalName) {
            case "io.debezium.time.Date":
                type = DataTypes.DATE();
                break;
            case "io.debezium.time.Time":
                type = DataTypes.TIME(3);
                break;
            case "io.debezium.time.MicroTime":
                type = DataTypes.TIME(6);
                break;
            case "io.debezium.time.NanoTime":
                type = DataTypes.TIME(9);
                break;
            case "io.debezium.time.Timestamp":
                type = DataTypes.TIMESTAMP(3);
                break;
            case "io.debezium.time.MicroTimestamp":
                type = DataTypes.TIMESTAMP(6);
                break;
            case "io.debezium.time.NanoTimestamp":
                type = DataTypes.TIMESTAMP(9);
                break;
            case "io.debezium.time.ZonedTimestamp":
                type = DataTypes.TIMESTAMP_TZ(9);
                break;
            case "io.debezium.time.Year":
                type = DataTypes.INT();
                break;
            case "io.debezium.data.Bits":
                type =
                        DataTypes.VARBINARY(
                                positiveParameter(schema, "length").orElse(Integer.MAX_VALUE));
                break;
            case "io.debezium.data.Enum":
            case "io.debezium.data.Json":
                type = DataTypes.STRING();
                break;
            case "org.apache.kafka.connect.data.Decimal":
                int scale = schema.path("parameters").path("scale").asInt(0);
                int precision =
                        schema.path("parameters").path("connect.decimal.precision").asInt(38);
                type = DataTypes.DECIMAL(Math.min(38, precision), Math.min(scale, precision));
                break;
            default:
                type = parsePrimitiveType(schema);
        }
        return schema.path("optional").asBoolean(true) ? type.nullable() : type.notNull();
    }

    private DataType parsePrimitiveType(JsonNode schema) {
        String type = schema.path("type").asText();
        switch (type) {
            case "int8":
                return DataTypes.TINYINT();
            case "int16":
                return DataTypes.SMALLINT();
            case "int32":
                return DataTypes.INT();
            case "int64":
                return DataTypes.BIGINT();
            case "float":
            case "float32":
                return DataTypes.FLOAT();
            case "double":
            case "float64":
                return DataTypes.DOUBLE();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "bytes":
                return DataTypes.BYTES();
            case "string":
                // Kafka Connect has no VARCHAR; MySQL CHAR/VARCHAR/TEXT all become string.
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException(
                        "Unsupported Debezium schema type '" + type + "'.");
        }
    }

    private Optional<Integer> positiveParameter(JsonNode schema, String name) {
        JsonNode value = schema.path("parameters").path(name);
        if (value.isMissingNode() || value.isNull()) {
            return Optional.empty();
        }
        try {
            int parsed = Integer.parseInt(value.asText());
            return parsed > 0 ? Optional.of(parsed) : Optional.empty();
        } catch (NumberFormatException ignored) {
            return Optional.empty();
        }
    }

    private String requiredText(JsonNode node, String field, String description) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isEmpty()) {
            throw new IllegalArgumentException(description + " is missing '" + field + "'.");
        }
        return value.asText();
    }
}
