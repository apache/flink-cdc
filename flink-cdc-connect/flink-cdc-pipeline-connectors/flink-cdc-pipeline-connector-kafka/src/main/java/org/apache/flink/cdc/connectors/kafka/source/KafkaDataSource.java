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

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DataSource} for reading from Kafka topics with Canal JSON format. */
@Internal
public class KafkaDataSource implements DataSource {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final Properties kafkaProperties;
    private final String groupId;
    private final StartupMode startupMode;
    @Nullable private final String specificOffsets;
    @Nullable private final Long startupTimestampMillis;
    private final CanalJsonDeserializationSchema deserializationSchema;

    public KafkaDataSource(
            String topic,
            Properties kafkaProperties,
            String groupId,
            StartupMode startupMode,
            @Nullable String specificOffsets,
            @Nullable Long startupTimestampMillis,
            CanalJsonDeserializationSchema deserializationSchema) {
        this.topic = checkNotNull(topic, "topic must not be null");
        this.kafkaProperties = checkNotNull(kafkaProperties, "kafkaProperties must not be null");
        this.groupId = checkNotNull(groupId, "groupId must not be null");
        this.startupMode = checkNotNull(startupMode, "startupMode must not be null");
        this.specificOffsets = specificOffsets;
        this.startupTimestampMillis = startupTimestampMillis;
        this.deserializationSchema =
                checkNotNull(deserializationSchema, "deserializer must not be null");
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        KafkaSourceBuilder<Event> sourceBuilder = KafkaSource.<Event>builder();

        // Set bootstrap servers from kafka properties
        if (kafkaProperties.containsKey("bootstrap.servers")) {
            sourceBuilder.setBootstrapServers(kafkaProperties.get("bootstrap.servers").toString());
        }

        // Set topics (supports comma-separated list)
        List<String> topics = Arrays.asList(topic.split(","));
        sourceBuilder.setTopics(topics.toArray(new String[0]));

        // Set group id
        sourceBuilder.setGroupId(groupId);

        // Set kafka properties
        sourceBuilder.setProperties(kafkaProperties);

        // Set starting offsets based on startup mode
        sourceBuilder.setStartingOffsets(getOffsetsInitializer());

        // Set deserializer
        sourceBuilder.setDeserializer(deserializationSchema);

        KafkaSource<Event> source = sourceBuilder.build();
        return FlinkSourceProvider.of(source);
    }

    private OffsetsInitializer getOffsetsInitializer() {
        switch (startupMode) {
            case EARLIEST_OFFSET:
                return OffsetsInitializer.earliest();
            case LATEST_OFFSET:
                return OffsetsInitializer.latest();
            case TIMESTAMP:
                if (startupTimestampMillis == null) {
                    throw new IllegalArgumentException(
                            "scan.startup.timestamp-millis must be set when startup mode is 'timestamp'");
                }
                return OffsetsInitializer.timestamp(startupTimestampMillis);
            case SPECIFIC_OFFSETS:
                if (specificOffsets == null) {
                    throw new IllegalArgumentException(
                            "scan.startup.specific-offsets must be set when startup mode is 'specific-offsets'");
                }
                return parseSpecificOffsets(specificOffsets);
            case GROUP_OFFSETS:
            default:
                return OffsetsInitializer.committedOffsets();
        }
    }

    private OffsetsInitializer parseSpecificOffsets(String offsetsStr) {
        Map<org.apache.kafka.common.TopicPartition, Long> offsets = new HashMap<>();
        String[] parts = offsetsStr.split(";");
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // Format: partition:0,offset:42
            String[] keyValuePairs = trimmed.split(",");
            int partition = -1;
            long offset = -1;
            for (String kv : keyValuePairs) {
                String[] pair = kv.trim().split(":");
                if (pair.length == 2) {
                    if (pair[0].trim().equals("partition")) {
                        partition = Integer.parseInt(pair[1].trim());
                    } else if (pair[0].trim().equals("offset")) {
                        offset = Long.parseLong(pair[1].trim());
                    }
                }
            }
            if (partition >= 0 && offset >= 0) {
                offsets.put(new org.apache.kafka.common.TopicPartition(topic, partition), offset);
            }
        }
        return OffsetsInitializer.offsets(offsets);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        // Kafka does not maintain schemas, return a no-op metadata accessor
        return new KafkaMetadataAccessor(topic);
    }

    @Override
    public boolean isParallelMetadataSource() {
        // Kafka can have schema changes on different partitions/tables in parallel
        return true;
    }
}
