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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * A {@link KafkaRecordDeserializationSchema} to deserialize Kafka records into pipeline {@link
 * Event}s.
 *
 * <p>Tombstone records are skipped. Value bytes are parsed by the configured {@code value.format}.
 */
public class PipelineKafkaRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private final JsonSerializationType valueFormat;
    private final DebeziumJsonDeserializationSchema debeziumDeserialization;
    private final CanalJsonDeserializationSchema canalDeserialization;

    public PipelineKafkaRecordDeserializationSchema() {
        this(JsonSerializationType.DEBEZIUM_JSON, null, null);
    }

    public PipelineKafkaRecordDeserializationSchema(String tables, String tablesExclude) {
        this(JsonSerializationType.DEBEZIUM_JSON, tables, tablesExclude);
    }

    public PipelineKafkaRecordDeserializationSchema(
            JsonSerializationType valueFormat, String tables, String tablesExclude) {
        this.valueFormat = valueFormat == null ? JsonSerializationType.DEBEZIUM_JSON : valueFormat;
        switch (this.valueFormat) {
            case CANAL_JSON:
                this.canalDeserialization =
                        new CanalJsonDeserializationSchema(tables, tablesExclude);
                this.debeziumDeserialization = null;
                break;
            case DEBEZIUM_JSON:
            default:
                this.debeziumDeserialization =
                        new DebeziumJsonDeserializationSchema(tables, tablesExclude);
                this.canalDeserialization = null;
        }
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        if (debeziumDeserialization != null) {
            debeziumDeserialization.open();
        } else {
            canalDeserialization.open();
        }
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out)
            throws IOException {
        if (record.value() == null) {
            return;
        }
        if (valueFormat == JsonSerializationType.CANAL_JSON) {
            canalDeserialization.deserialize(record, out);
        } else {
            debeziumDeserialization.deserialize(record, out);
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
