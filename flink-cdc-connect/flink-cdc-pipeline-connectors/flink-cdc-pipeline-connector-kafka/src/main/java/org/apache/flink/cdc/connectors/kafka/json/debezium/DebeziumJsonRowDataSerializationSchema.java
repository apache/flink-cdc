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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.connectors.kafka.utils.JsonRowDataSerializationSchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonParserRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a JSON
 * bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * JsonRowDataDeserializationSchema} or {@link JsonParserRowDataDeserializationSchema}.
 */
public class DebeziumJsonRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    /** The converter that converts internal data formats to JsonNode. */
    private final RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter;

    /** Object mapper that is used to create output JSON objects. */
    private transient ObjectMapper mapper;

    /** Reusable object node. */
    private transient ObjectNode node;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. */
    private final String mapNullKeyLiteral;

    /** Flag indicating whether to serialize all decimals as plain numbers. */
    private final boolean encodeDecimalAsPlainNumber;

    /** Flag indicating whether to ignore null fields. */
    private final boolean ignoreNullFields;

    private final boolean isIncludedDebeziumSchema;

    public DebeziumJsonRowDataSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields,
            boolean isIncludedDebeziumSchema) {
        this.rowType = rowType;
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.runtimeConverter =
                JsonRowDataSerializationSchemaUtils.createRowDataToJsonConverters(
                                timestampFormat,
                                mapNullKeyMode,
                                mapNullKeyLiteral,
                                ignoreNullFields)
                        .createConverter(rowType);
        this.ignoreNullFields = ignoreNullFields;
        this.isIncludedDebeziumSchema = isIncludedDebeziumSchema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(
                                JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN,
                                encodeDecimalAsPlainNumber);
    }

    @Override
    public byte[] serialize(RowData row) {
        if (node == null || ignoreNullFields) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, row);
            if (isIncludedDebeziumSchema) {
                // schema is a nested json string, asText() can return a pure string without other
                // escape characters such as "\"
                String schemaValue = node.get("schema").asText();
                JsonNode schemaNode = mapper.readTree(schemaValue);
                node.set("schema", schemaNode);
                return mapper.writeValueAsBytes(node);
            }
            return mapper.writeValueAsBytes(node);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Could not serialize row '%s'.", row), t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumJsonRowDataSerializationSchema that = (DebeziumJsonRowDataSerializationSchema) o;
        return rowType.equals(that.rowType)
                && timestampFormat.equals(that.timestampFormat)
                && mapNullKeyMode.equals(that.mapNullKeyMode)
                && mapNullKeyLiteral.equals(that.mapNullKeyLiteral)
                && encodeDecimalAsPlainNumber == that.encodeDecimalAsPlainNumber
                && ignoreNullFields == that.ignoreNullFields
                && isIncludedDebeziumSchema == that.isIncludedDebeziumSchema;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rowType,
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral,
                encodeDecimalAsPlainNumber,
                ignoreNullFields,
                isIncludedDebeziumSchema);
    }
}
