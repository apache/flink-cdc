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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;

import java.util.Optional;

/**
 * Util class to set parameters for {@link com.ververica.cdc.connectors.kafka.sink.KafkaDataSink}.
 */
public class KafkaOptionsUtil {

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_DEFAULT = "default";
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

    /**
     * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class
     * name.
     */
    public static Optional<FlinkKafkaPartitioner<Event>> getFlinkKafkaPartitioner(
            Configuration tableOptions, ClassLoader classLoader) {
        return tableOptions
                .getOptional(KafkaDataSinkOptions.SINK_PARTITIONER)
                .flatMap(
                        (String partitioner) -> {
                            switch (partitioner) {
                                case SINK_PARTITIONER_VALUE_FIXED:
                                    return Optional.of(new FlinkFixedPartitioner<>());
                                case SINK_PARTITIONER_VALUE_DEFAULT:
                                case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                                    return Optional.empty();
                                    // Default fallback to full class name of the partitioner.
                                default:
                                    return Optional.of(
                                            initializePartitioner(partitioner, classLoader));
                            }
                        });
    }

    /** Returns a class value with the given class name. */
    private static <T> FlinkKafkaPartitioner<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!FlinkKafkaPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink partitioner class '%s' should extend from the required class %s",
                                name, FlinkKafkaPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final FlinkKafkaPartitioner<T> kafkaPartitioner =
                    InstantiationUtil.instantiate(name, FlinkKafkaPartitioner.class, classLoader);

            return kafkaPartitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }
}
