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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.serialization.CsvSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.serialization.JsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.utils.JsonRowDataSerializationSchemaUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;

import java.time.ZoneId;

import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.debezium.DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL;

/**
 * Format factory for providing configured instances of {@link SerializationSchema} to convert
 * {@link Event} to byte.
 */
public class KeySerializationFactory {

    /**
     * Creates a configured instance of {@link SerializationSchema} to convert {@link Event} to
     * byte.
     */
    public static SerializationSchema<Event> createSerializationSchema(
            ReadableConfig formatOptions, KeyFormat keyFormat, ZoneId zoneId) {
        switch (keyFormat) {
            case JSON:
                {
                    TimestampFormat timestampFormat =
                            JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
                    JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                            JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
                    String mapNullKeyLiteral = formatOptions.get(JSON_MAP_NULL_KEY_LITERAL);

                    final boolean encodeDecimalAsPlainNumber =
                            formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
                    final boolean ignoreNullFields =
                            JsonRowDataSerializationSchemaUtils.enableIgnoreNullFields(
                                    formatOptions);
                    return new JsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            zoneId,
                            encodeDecimalAsPlainNumber,
                            ignoreNullFields);
                }
            case CSV:
                {
                    return new CsvSerializationSchema(zoneId);
                }
            default:
                {
                    throw new IllegalArgumentException("UnSupport key format of " + keyFormat);
                }
        }
    }
}
