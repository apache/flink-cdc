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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import com.ververica.cdc.connectors.kafka.json.JsonSerializationType;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.ververica.cdc.connectors.kafka.sink.KafkaDataSinkOptions.PROPERTIES_PREFIX;

/** A dummy {@link DataSinkFactory} to create {@link KafkaDataSink}. */
public class KafkaDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSink createDataSink(Context context) {
        Configuration configuration =
                Configuration.fromMap(context.getFactoryConfiguration().toMap());
        DeliveryGuarantee deliveryGuarantee =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        FlinkKafkaPartitioner<Event> partitioner =
                KafkaOptionsUtil.getFlinkKafkaPartitioner(
                                context.getFactoryConfiguration(),
                                KafkaDataSinkFactory.class.getClassLoader())
                        .orElse(null);
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        JsonSerializationType jsonSerializationType =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.VALUE_FORMAT);
        SerializationSchema<Event> valueSerialization =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        configuration, jsonSerializationType);
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
        return new KafkaDataSink(
                deliveryGuarantee,
                kafkaProperties,
                partitioner,
                zoneId,
                valueSerialization,
                topic,
                addTableToHeaderEnabled);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        options.add(KafkaDataSinkOptions.SINK_PARTITIONER);
        options.add(KafkaDataSinkOptions.TOPIC);
        options.add(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED);
        return options;
    }
}
