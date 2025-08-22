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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.utils.JsonRowDataSerializationSchemaUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;

import java.time.ZoneId;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA_ENABLED;
import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.debezium.DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL;

/**
 * Format factory for providing configured instances of {@link SerializationSchema} to convert
 * {@link Event} to json.
 */
@Internal
public class ChangeLogJsonFormatFactory {

    /**
     * Creates a configured instance of {@link SerializationSchema} to convert {@link Event} to
     * json.
     *
     * @param formatOptions The format options.
     * @param type The type of json serialization.
     * @return The configured instance of {@link SerializationSchema}.
     */
    public static SerializationSchema<Event> createSerializationSchema(
            ReadableConfig formatOptions, JsonSerializationType type, ZoneId zoneId) {
        final String prefix = type.toString() + ".";
        boolean isIncludedDebeziumSchema =
                Boolean.parseBoolean(
                        formatOptions
                                .toMap()
                                .get(
                                        DEBEZIUM_JSON_INCLUDE_SCHEMA_ENABLED
                                                .key()
                                                .substring(prefix.length())));

        TimestampFormat timestampFormat = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(JSON_MAP_NULL_KEY_LITERAL);

        final boolean encodeDecimalAsPlainNumber =
                formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);

        final boolean ignoreNullFields =
                JsonRowDataSerializationSchemaUtils.enableIgnoreNullFields(formatOptions);

        switch (type) {
            case DEBEZIUM_JSON:
                {
                    return new DebeziumJsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            zoneId,
                            encodeDecimalAsPlainNumber,
                            ignoreNullFields,
                            isIncludedDebeziumSchema);
                }
            case CANAL_JSON:
                {
                    return new CanalJsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            zoneId,
                            encodeDecimalAsPlainNumber,
                            ignoreNullFields);
                }
            default:
                {
                    throw new IllegalArgumentException(
                            "unSupport JsonSerializationType of " + type);
                }
        }
    }
}
