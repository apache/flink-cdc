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
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.connectors.kafka.utils.KafkaSinkUtils;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link KafkaRecordSerializationSchema} to serialize {@link Event}.
 *
 * <p>The topic to be sent is the string value of {@link TableId}.
 *
 * <p>the key of {@link ProducerRecord} is null as we don't need to upsert Kafka.
 */
public class PipelineKafkaRecordSerializationSchema
        implements KafkaRecordSerializationSchema<Event> {

    private final Integer partition;

    private final SerializationSchema<Event> keySerialization;

    private final SerializationSchema<Event> valueSerialization;

    private final String unifiedTopic;

    private final boolean addTableToHeaderEnabled;

    // key value pairs to be put into Kafka Record Header.
    public final Map<String, String> customHeaders;

    private final String mappingRuleString;

    private Map<Selectors, String> selectorsToTopicMap;

    // A cache to speed up TableId to Topic mapping.
    private Map<TableId, String> tableIdToTopicCache;

    public static final String NAMESPACE_HEADER_KEY = "namespace";

    public static final String SCHEMA_NAME_HEADER_KEY = "schemaName";

    public static final String TABLE_NAME_HEADER_KEY = "tableName";

    PipelineKafkaRecordSerializationSchema(
            PartitionStrategy partitionStrategy,
            SerializationSchema<Event> keySerialization,
            SerializationSchema<Event> valueSerialization,
            String unifiedTopic,
            boolean addTableToHeaderEnabled,
            String customHeaderString,
            String mappingRuleString) {
        this.keySerialization = keySerialization;
        this.valueSerialization = checkNotNull(valueSerialization);
        this.unifiedTopic = unifiedTopic;
        this.addTableToHeaderEnabled = addTableToHeaderEnabled;
        customHeaders = new HashMap<>();
        if (!customHeaderString.isEmpty()) {
            for (String tables : customHeaderString.split(";")) {
                String[] splits = tables.split(":");
                if (splits.length == 2) {
                    String headerKey = splits[0].trim();
                    String headerValue = splits[1].trim();
                    customHeaders.put(headerKey, headerValue);
                } else {
                    throw new IllegalArgumentException(
                            KafkaDataSinkOptions.SINK_CUSTOM_HEADER
                                    + " is malformed, please refer to the documents");
                }
            }
        }
        partition = partitionStrategy.equals(PartitionStrategy.ALL_TO_ZERO) ? 0 : null;
        this.mappingRuleString = mappingRuleString;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Event event, KafkaSinkContext context, Long timestamp) {
        ChangeEvent changeEvent = (ChangeEvent) event;
        final byte[] keySerialized = keySerialization.serialize(event);
        final byte[] valueSerialized = valueSerialization.serialize(event);
        if (event instanceof SchemaChangeEvent) {
            // skip sending SchemaChangeEvent.
            return null;
        }
        String topic = inferTopicName(changeEvent.tableId());
        RecordHeaders recordHeaders = new RecordHeaders();
        if (addTableToHeaderEnabled) {
            String namespace =
                    changeEvent.tableId().getNamespace() == null
                            ? ""
                            : changeEvent.tableId().getNamespace();
            recordHeaders.add(new RecordHeader(NAMESPACE_HEADER_KEY, namespace.getBytes(UTF_8)));
            String schemaName =
                    changeEvent.tableId().getSchemaName() == null
                            ? ""
                            : changeEvent.tableId().getSchemaName();
            recordHeaders.add(new RecordHeader(SCHEMA_NAME_HEADER_KEY, schemaName.getBytes(UTF_8)));
            String tableName = changeEvent.tableId().getTableName();
            recordHeaders.add(new RecordHeader(TABLE_NAME_HEADER_KEY, tableName.getBytes(UTF_8)));
        }
        if (!this.customHeaders.isEmpty()) {
            for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
                recordHeaders.add(
                        new RecordHeader(entry.getKey(), entry.getValue().getBytes(UTF_8)));
            }
        }
        return new ProducerRecord<>(
                topic, partition, null, keySerialized, valueSerialized, recordHeaders);
    }

    private String inferTopicName(TableId tableId) {
        return tableIdToTopicCache.computeIfAbsent(
                tableId,
                (table -> {
                    if (unifiedTopic != null && !unifiedTopic.isEmpty()) {
                        return unifiedTopic;
                    }
                    if (selectorsToTopicMap != null && !selectorsToTopicMap.isEmpty()) {
                        for (Map.Entry<Selectors, String> entry : selectorsToTopicMap.entrySet()) {
                            if (entry.getKey().isMatch(tableId)) {
                                return entry.getValue();
                            }
                        }
                    }
                    return table.toString();
                }));
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        this.selectorsToTopicMap = KafkaSinkUtils.parseSelectorsToTopicMap(mappingRuleString);
        this.tableIdToTopicCache = new HashMap<>();
        valueSerialization.open(context);
    }
}
