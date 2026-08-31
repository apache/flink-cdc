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

import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses {@code scan.startup.specific-offsets} into Kafka topic-partition offsets.
 *
 * <p>Supported entries are {@code partition:0,offset:42} when a default topic is provided, or
 * {@code topic:dbz.customers,partition:0,offset:42}. Multiple entries are separated by {@code ;}.
 */
class KafkaStartupOffsets {

    private KafkaStartupOffsets() {}

    static Map<TopicPartition, Long> parse(String spec, @Nullable String defaultTopic) {
        if (spec == null || spec.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Option 'scan.startup.specific-offsets' must not be empty.");
        }
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        for (String rawEntry : spec.split(";")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            String topic = defaultTopic;
            Integer partition = null;
            Long offset = null;
            for (String rawPart : entry.split(",")) {
                String part = rawPart.trim();
                int colon = part.indexOf(':');
                if (colon <= 0 || colon == part.length() - 1) {
                    throw new IllegalArgumentException(
                            "Invalid specific-offsets entry '" + entry + "'.");
                }
                String key = part.substring(0, colon).trim();
                String value = part.substring(colon + 1).trim();
                switch (key) {
                    case "topic":
                        topic = value;
                        break;
                    case "partition":
                        partition = parseInteger(value, "partition", entry);
                        break;
                    case "offset":
                        offset = parseLong(value, "offset", entry);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown key '"
                                        + key
                                        + "' in specific-offsets entry '"
                                        + entry
                                        + "'.");
                }
            }
            if (topic == null || topic.isEmpty()) {
                throw new IllegalArgumentException(
                        "Each specific-offsets entry must include 'topic' unless exactly one topic is configured.");
            }
            if (partition == null || offset == null) {
                throw new IllegalArgumentException(
                        "Each specific-offsets entry must include 'partition' and 'offset': '"
                                + entry
                                + "'.");
            }
            result.put(new TopicPartition(topic, partition), offset);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(
                    "Option 'scan.startup.specific-offsets' must contain at least one entry.");
        }
        return result;
    }

    private static int parseInteger(String value, String name, String entry) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid "
                            + name
                            + " '"
                            + value
                            + "' in specific-offsets entry '"
                            + entry
                            + "'.",
                    e);
        }
    }

    private static long parseLong(String value, String name, String entry) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid "
                            + name
                            + " '"
                            + value
                            + "' in specific-offsets entry '"
                            + entry
                            + "'.",
                    e);
        }
    }
}
