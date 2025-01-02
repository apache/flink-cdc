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

package org.apache.flink.cdc.connectors.kafka.format.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.connectors.kafka.format.FormatFactory;
import org.apache.flink.cdc.connectors.kafka.format.JsonFormatOptionsUtil;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.ValidationException;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.kafka.format.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL;
import static org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_MODE;
import static org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatOptions.SCHEMA_INCLUDE;
import static org.apache.flink.cdc.connectors.kafka.format.debezium.DebeziumJsonFormatOptions.TIMESTAMP_FORMAT;

/** Format factory for providing configured instances of Debezium JSON to Event. */
public class DebeziumJsonFormatFactory implements FormatFactory {

    public static final String IDENTIFIER = "debezium-json";

    @Override
    public SerializationSchema<Event> createEncodingFormat(
            Context context, Configuration formatOptions) {
        FactoryHelper.validateFactoryOptions(this, context.getFactoryConfiguration());
        validateEncodingFormatOptions(formatOptions);

        TimestampFormat timestampFormat = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(JSON_MAP_NULL_KEY_LITERAL);

        Boolean encodeDecimalAsPlainNumber = formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);

        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        return new DebeziumJsonSerializationSchema(
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral,
                zoneId,
                encodeDecimalAsPlainNumber);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEMA_INCLUDE);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(JSON_MAP_NULL_KEY_MODE);
        options.add(JSON_MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        return options;
    }

    /** Validator for debezium encoding format. */
    private static void validateEncodingFormatOptions(Configuration tableOptions) {
        JsonFormatOptionsUtil.validateEncodingFormatOptions(tableOptions);

        // validator for {@link SCHEMA_INCLUDE}
        if (tableOptions.get(DebeziumJsonFormatOptions.SCHEMA_INCLUDE)) {
            throw new ValidationException(
                    String.format(
                            "Debezium JSON serialization doesn't support '%s.%s' option been set to true.",
                            IDENTIFIER, DebeziumJsonFormatOptions.SCHEMA_INCLUDE.key()));
        }
    }
}
