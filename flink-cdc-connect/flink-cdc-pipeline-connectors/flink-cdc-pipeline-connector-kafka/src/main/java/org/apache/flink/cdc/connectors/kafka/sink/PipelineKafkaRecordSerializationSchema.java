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
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.annotation.Nullable;

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
    private final FlinkKafkaPartitioner<Event> partitioner;
    private final SerializationSchema<Event> valueSerialization;

    private final String unifiedTopic;

    private final boolean addTableToHeaderEnabled;

    // key value pairs to be put into Kafka Record Header.
    public final Map<String, String> customHeaders;

    public static final String NAMESPACE_HEADER_KEY = "namespace";

    public static final String SCHEMA_NAME_HEADER_KEY = "schemaName";

    public static final String TABLE_NAME_HEADER_KEY = "tableName";

    PipelineKafkaRecordSerializationSchema(
            @Nullable FlinkKafkaPartitioner<Event> partitioner,
            SerializationSchema<Event> valueSerialization,
            String unifiedTopic,
            boolean addTableToHeaderEnabled,
            String customHeaderString) {
        this.partitioner = partitioner;
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
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Event event, KafkaSinkContext context, Long timestamp) {
        ChangeEvent changeEvent = (ChangeEvent) event;
        final byte[] valueSerialized = valueSerialization.serialize(event);
        if (event instanceof SchemaChangeEvent) {
            // skip sending SchemaChangeEvent.
            return null;
        }
        String topic = unifiedTopic == null ? changeEvent.tableId().toString() : unifiedTopic;
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
                topic,
                extractPartition(
                        changeEvent, valueSerialized, context.getPartitionsForTopic(topic)),
                null,
                null,
                valueSerialized,
                recordHeaders);
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    private Integer extractPartition(
            ChangeEvent changeEvent, byte[] valueSerialized, int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    changeEvent,
                    null,
                    valueSerialized,
                    changeEvent.tableId().toString(),
                    partitions);
        }
        return null;
    }
}
