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

import org.apache.flink.connector.base.DeliveryGuarantee;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.description.Description;
import com.ververica.cdc.connectors.kafka.json.JsonSerializationType;

import static com.ververica.cdc.common.configuration.ConfigOptions.key;
import static com.ververica.cdc.common.configuration.description.TextElement.text;

/** Options for {@link KafkaDataSinkOptions}. */
public class KafkaDataSinkOptions {

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<JsonSerializationType> VALUE_FORMAT =
            key("value.format")
                    .enumType(JsonSerializationType.class)
                    .defaultValue(JsonSerializationType.DEBEZIUM_JSON)
                    .withDescription(
                            "Defines the format identifier for encoding value data, "
                                    + "available options are `debezium-json` and `canal-json`, default option is `debezium-json`.");

    public static final ConfigOption<String> SINK_PARTITIONER =
            key("sink.partitioner")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional output partitioning from Flink's partitions into Kafka's partitions. Valid enumerations are")
                                    .list(
                                            text(
                                                    "'default' (use kafka default partitioner to partition records)"),
                                            text(
                                                    "'fixed' (each Flink partition ends up in at most one Kafka partition)"),
                                            text(
                                                    "'round-robin' (a Flink partition is distributed to Kafka partitions round-robin when 'key.fields' is not specified)"),
                                            text(
                                                    "custom class name (use custom FlinkKafkaPartitioner subclass)"))
                                    .build());

    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional. If this parameter is configured, all events will be sent to this topic.");

    public static final ConfigOption<Boolean> SINK_ADD_TABLEID_TO_HEADER_ENABLED =
            key("sink.add-tableId-to-header-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional. If this parameter is configured, a header with key of 'namespace','schemaName','tableName' will be added for each Kafka record.");
}
