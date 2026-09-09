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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/** A pipeline {@link DataSource} backed by Flink's {@link KafkaSource}. */
@Internal
public class KafkaDataSource implements DataSource {

    private final List<String> topics;
    private final @Nullable String topicPattern;
    private final Properties properties;
    private final String startupMode;
    private final @Nullable Long startupTimestampMillis;
    private final Map<TopicPartition, Long> specificOffsets;
    private final @Nullable String tables;
    private final @Nullable String tablesExclude;
    private final JsonSerializationType valueFormat;

    KafkaDataSource(
            List<String> topics,
            @Nullable String topicPattern,
            Properties properties,
            String startupMode,
            @Nullable Long startupTimestampMillis,
            Map<TopicPartition, Long> specificOffsets,
            @Nullable String tables,
            @Nullable String tablesExclude,
            JsonSerializationType valueFormat) {
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.properties = properties;
        this.startupMode = startupMode;
        this.startupTimestampMillis = startupTimestampMillis;
        this.specificOffsets = specificOffsets;
        this.tables = tables;
        this.tablesExclude = tablesExclude;
        this.valueFormat = valueFormat;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        KafkaSourceBuilder<Event> builder =
                KafkaSource.<Event>builder()
                        .setProperties(properties)
                        .setDeserializer(
                                new PipelineKafkaRecordDeserializationSchema(
                                        valueFormat, tables, tablesExclude));
        if (topicPattern == null) {
            builder.setTopics(topics);
        } else {
            builder.setTopicPattern(Pattern.compile(topicPattern));
        }
        switch (startupMode.toLowerCase(Locale.ROOT)) {
            case "earliest-offset":
                builder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case "latest-offset":
                builder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case "group-offsets":
                builder.setStartingOffsets(OffsetsInitializer.committedOffsets());
                break;
            case "timestamp":
                builder.setStartingOffsets(OffsetsInitializer.timestamp(startupTimestampMillis));
                break;
            case "specific-offsets":
                builder.setStartingOffsets(OffsetsInitializer.offsets(specificOffsets));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported scan.startup.mode '"
                                + startupMode
                                + "'. Supported values are earliest-offset, latest-offset, "
                                + "group-offsets, timestamp, and specific-offsets.");
        }
        return FlinkSourceProvider.of(builder.build());
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MetadataAccessor() {
            private UnsupportedOperationException unsupported() {
                return new UnsupportedOperationException(
                        "Kafka source discovers table metadata from consumed records.");
            }

            @Override
            public List<String> listNamespaces() {
                throw unsupported();
            }

            @Override
            public List<String> listSchemas(@Nullable String namespace) {
                throw unsupported();
            }

            @Override
            public List<TableId> listTables(
                    @Nullable String namespace, @Nullable String schemaName) {
                throw unsupported();
            }

            @Override
            public Schema getTableSchema(TableId tableId) {
                throw unsupported();
            }
        };
    }

    @Override
    public boolean isParallelMetadataSource() {
        return true;
    }

    List<String> getTopics() {
        return topics;
    }

    @Nullable
    String getTopicPattern() {
        return topicPattern;
    }

    Properties getProperties() {
        return properties;
    }

    String getStartupMode() {
        return startupMode;
    }

    @Nullable
    Long getStartupTimestampMillis() {
        return startupTimestampMillis;
    }

    Map<TopicPartition, Long> getSpecificOffsets() {
        return Collections.unmodifiableMap(specificOffsets);
    }

    @Nullable
    String getTables() {
        return tables;
    }

    @Nullable
    String getTablesExclude() {
        return tablesExclude;
    }

    JsonSerializationType getValueFormat() {
        return valueFormat;
    }
}
