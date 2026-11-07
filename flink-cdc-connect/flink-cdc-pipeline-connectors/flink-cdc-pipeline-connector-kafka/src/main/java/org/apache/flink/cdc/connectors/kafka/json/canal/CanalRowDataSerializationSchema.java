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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map.Entry;

import static java.lang.String.format;

/** Serializes the envelope while retaining only changed fields in the old row. */
final class CanalRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowDataToJsonConverter converter;
    private final boolean encodeDecimalAsPlainNumber;
    private transient ObjectMapper mapper;

    CanalRowDataSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields) {
        this.converter =
                new RowDataToJsonConverters(
                                timestampFormat,
                                mapNullKeyMode,
                                mapNullKeyLiteral,
                                ignoreNullFields)
                        .createConverter(rowType);
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
    }

    @Override
    public void open(InitializationContext context) {
        mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(
                                JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN,
                                encodeDecimalAsPlainNumber);
    }

    @Override
    public byte[] serialize(RowData row) {
        // Do not reuse a pruned old row for subsequent events.
        ObjectNode node = mapper.createObjectNode();
        try {
            converter.convert(mapper, node, row);
            JsonNode old = node.get("old");
            if (old != null && !old.isNull()) {
                JsonNode after = node.get("data").get(0);
                Iterator<Entry<String, JsonNode>> fields = old.get(0).fields();
                while (fields.hasNext()) {
                    Entry<String, JsonNode> field = fields.next();
                    if (field.getValue().equals(after.get(field.getKey()))) {
                        fields.remove();
                    }
                }
            }
            return mapper.writeValueAsBytes(node);
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize row '%s'.", row), t);
        }
    }
}
