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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.table.api.ValidationException;

import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link Factory} for creating Kafka pipeline sources. */
@Internal
public class KafkaDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(KafkaDataSourceOptions.PROPERTIES_PREFIX);
        Configuration configuration = context.getFactoryConfiguration();
        boolean hasTopics = configuration.getOptional(KafkaDataSourceOptions.TOPIC).isPresent();
        boolean hasTopicPattern =
                configuration.getOptional(KafkaDataSourceOptions.TOPIC_PATTERN).isPresent();
        if (hasTopics == hasTopicPattern) {
            throw new ValidationException(
                    "Exactly one of options 'topic' and 'topic-pattern' must be configured.");
        }
        List<String> topics = Collections.emptyList();
        String topicPattern = null;
        if (hasTopics) {
            topics =
                    Arrays.stream(configuration.get(KafkaDataSourceOptions.TOPIC).split(","))
                            .map(String::trim)
                            .filter(topic -> !topic.isEmpty())
                            .collect(Collectors.toList());
            if (topics.isEmpty()) {
                throw new ValidationException("Option 'topic' must contain at least one topic.");
            }
        } else {
            topicPattern = configuration.get(KafkaDataSourceOptions.TOPIC_PATTERN).trim();
            if (topicPattern.isEmpty()) {
                throw new ValidationException("Option 'topic-pattern' must not be empty.");
            }
        }

        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
            if (entry.getKey().startsWith(KafkaDataSourceOptions.PROPERTIES_PREFIX)) {
                properties.setProperty(
                        entry.getKey().substring(KafkaDataSourceOptions.PROPERTIES_PREFIX.length()),
                        entry.getValue());
            }
        }
        configuration
                .getOptional(KafkaDataSourceOptions.GROUP_ID)
                .ifPresent(groupId -> properties.setProperty("group.id", groupId));
        if (!properties.containsKey("bootstrap.servers")) {
            throw new ValidationException(
                    "Kafka bootstrap servers must be configured with 'properties.bootstrap.servers'.");
        }
        if (!properties.containsKey("group.id")) {
            throw new ValidationException(
                    "Kafka consumer group must be configured with 'group-id' or 'properties.group.id'.");
        }

        String startupMode =
                configuration
                        .get(KafkaDataSourceOptions.SCAN_STARTUP_MODE)
                        .toLowerCase(Locale.ROOT);
        Long startupTimestampMillis =
                configuration
                        .getOptional(KafkaDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)
                        .orElse(null);
        String specificOffsetsSpec =
                configuration
                        .getOptional(KafkaDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSETS)
                        .orElse(null);
        Map<TopicPartition, Long> specificOffsets = Collections.emptyMap();
        switch (startupMode) {
            case "earliest-offset":
            case "latest-offset":
            case "group-offsets":
                if (startupTimestampMillis != null) {
                    throw new ValidationException(
                            "Option 'scan.startup.timestamp-millis' is only supported when 'scan.startup.mode' is 'timestamp'.");
                }
                if (specificOffsetsSpec != null) {
                    throw new ValidationException(
                            "Option 'scan.startup.specific-offsets' is only supported when 'scan.startup.mode' is 'specific-offsets'.");
                }
                break;
            case "timestamp":
                if (startupTimestampMillis == null) {
                    throw new ValidationException(
                            "Option 'scan.startup.timestamp-millis' is required when 'scan.startup.mode' is 'timestamp'.");
                }
                if (specificOffsetsSpec != null) {
                    throw new ValidationException(
                            "Option 'scan.startup.specific-offsets' is only supported when 'scan.startup.mode' is 'specific-offsets'.");
                }
                break;
            case "specific-offsets":
                if (specificOffsetsSpec == null || specificOffsetsSpec.trim().isEmpty()) {
                    throw new ValidationException(
                            "Option 'scan.startup.specific-offsets' is required when 'scan.startup.mode' is 'specific-offsets'.");
                }
                if (startupTimestampMillis != null) {
                    throw new ValidationException(
                            "Option 'scan.startup.timestamp-millis' is only supported when 'scan.startup.mode' is 'timestamp'.");
                }
                String defaultTopic = topics.size() == 1 ? topics.get(0) : null;
                try {
                    specificOffsets = KafkaStartupOffsets.parse(specificOffsetsSpec, defaultTopic);
                } catch (IllegalArgumentException e) {
                    throw new ValidationException(e.getMessage(), e);
                }
                break;
            default:
                throw new ValidationException(
                        "Unsupported scan.startup.mode '"
                                + startupMode
                                + "'. Supported values are earliest-offset, latest-offset, "
                                + "group-offsets, timestamp, and specific-offsets.");
        }

        return new KafkaDataSource(
                topics,
                topicPattern,
                properties,
                startupMode,
                startupTimestampMillis,
                specificOffsets,
                blankToNull(configuration.getOptional(KafkaDataSourceOptions.TABLES).orElse(null)),
                blankToNull(
                        configuration
                                .getOptional(KafkaDataSourceOptions.TABLES_EXCLUDE)
                                .orElse(null)),
                configuration.get(KafkaDataSourceOptions.VALUE_FORMAT));
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
        return new HashSet<>(
                Arrays.asList(
                        KafkaDataSourceOptions.TOPIC,
                        KafkaDataSourceOptions.TOPIC_PATTERN,
                        KafkaDataSourceOptions.GROUP_ID,
                        KafkaDataSourceOptions.SCAN_STARTUP_MODE,
                        KafkaDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        KafkaDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSETS,
                        KafkaDataSourceOptions.TABLES,
                        KafkaDataSourceOptions.TABLES_EXCLUDE,
                        KafkaDataSourceOptions.VALUE_FORMAT));
    }

    private static String blankToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }
}
