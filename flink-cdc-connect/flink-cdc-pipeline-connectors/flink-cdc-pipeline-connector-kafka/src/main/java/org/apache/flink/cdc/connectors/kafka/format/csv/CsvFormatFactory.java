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

package org.apache.flink.cdc.connectors.kafka.format.csv;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.connectors.kafka.format.FormatFactory;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.ALLOW_COMMENTS;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.QUOTE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION;

/** Format factory for providing configured instances of CSV to Event. */
public class CsvFormatFactory implements FormatFactory {

    public static final String IDENTIFIER = "csv";

    @Override
    public SerializationSchema<Event> createEncodingFormat(
            Context context, Configuration formatOptions) {
        FactoryHelper.validateFactoryOptions(this, context.getFactoryConfiguration());

        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        return new CsvSerializationSchema(zoneId, formatOptions);
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
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        options.add(WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION);
        return options;
    }
}
