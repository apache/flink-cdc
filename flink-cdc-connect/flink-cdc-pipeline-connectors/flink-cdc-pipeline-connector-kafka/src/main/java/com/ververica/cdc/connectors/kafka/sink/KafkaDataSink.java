/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.ZoneId;
import java.util.Properties;

/** A {@link DataSink} for "Kafka" connector. */
public class KafkaDataSink implements DataSink {

    final Properties kafkaProperties;

    final DeliveryGuarantee deliveryGuarantee;

    final FlinkKafkaPartitioner<Event> partitioner;

    final ZoneId zoneId;

    final SerializationSchema<Event> valueSerialization;

    final String topic;

    final boolean addTableToHeaderEnabled;

    public KafkaDataSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProperties,
            FlinkKafkaPartitioner<Event> partitioner,
            ZoneId zoneId,
            SerializationSchema<Event> valueSerialization,
            String topic,
            boolean addTableToHeaderEnabled) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProperties = kafkaProperties;
        this.partitioner = partitioner;
        this.zoneId = zoneId;
        this.valueSerialization = valueSerialization;
        this.topic = topic;
        this.addTableToHeaderEnabled = addTableToHeaderEnabled;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        final KafkaSinkBuilder<Event> sinkBuilder = KafkaSink.builder();
        return FlinkSinkProvider.of(
                sinkBuilder
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setBootstrapServers(
                                kafkaProperties
                                        .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
                                        .toString())
                        .setKafkaProducerConfig(kafkaProperties)
                        .setRecordSerializer(
                                new PipelineKafkaRecordSerializationSchema(
                                        partitioner,
                                        valueSerialization,
                                        topic,
                                        addTableToHeaderEnabled))
                        .build());
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        // simply do nothing here because Kafka do not maintain the schemas.
        return schemaChangeEvent -> {};
    }
}
