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
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA_ENABLED;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.KEY_FORMAT;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.PARTITION_STRATEGY;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.SINK_CUSTOM_HEADER;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.SINK_TABLE_ID_TO_TOPIC_MAPPING;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.TOPIC;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.VALUE_FORMAT;

/** A dummy {@link DataSinkFactory} to create {@link KafkaDataSink}. */
public class KafkaDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSink createDataSink(Context context) {
        KeyFormat keyFormat = context.getFactoryConfiguration().get(KEY_FORMAT);
        JsonSerializationType jsonSerializationType =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.VALUE_FORMAT);

        FactoryHelper helper = FactoryHelper.createFactoryHelper(this, context);
        helper.validateExcept(
                PROPERTIES_PREFIX, keyFormat.toString(), jsonSerializationType.toString());

        DeliveryGuarantee deliveryGuarantee =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        SerializationSchema<Event> keySerialization =
                KeySerializationFactory.createSerializationSchema(
                        helper.getFormatConfig(keyFormat.toString()), keyFormat, zoneId);
        SerializationSchema<Event> valueSerialization =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        helper.getFormatConfig(jsonSerializationType.toString()),
                        jsonSerializationType,
                        zoneId);
        final Properties kafkaProperties = new Properties();
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        allOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = allOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            kafkaProperties.put(subKey, value);
                        });
        String topic = context.getFactoryConfiguration().get(KafkaDataSinkOptions.TOPIC);
        boolean addTableToHeaderEnabled =
                context.getFactoryConfiguration()
                        .get(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED);
        String customHeaders =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.SINK_CUSTOM_HEADER);
        PartitionStrategy partitionStrategy =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.PARTITION_STRATEGY);
        String tableMapping = context.getFactoryConfiguration().get(SINK_TABLE_ID_TO_TOPIC_MAPPING);
        return new KafkaDataSink(
                deliveryGuarantee,
                kafkaProperties,
                partitionStrategy,
                zoneId,
                keySerialization,
                valueSerialization,
                topic,
                addTableToHeaderEnabled,
                customHeaders,
                tableMapping);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KEY_FORMAT);
        options.add(VALUE_FORMAT);
        options.add(PARTITION_STRATEGY);
        options.add(TOPIC);
        options.add(SINK_ADD_TABLEID_TO_HEADER_ENABLED);
        options.add(SINK_CUSTOM_HEADER);
        options.add(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        options.add(SINK_TABLE_ID_TO_TOPIC_MAPPING);
        options.add(DEBEZIUM_JSON_INCLUDE_SCHEMA_ENABLED);
        return options;
    }
}
