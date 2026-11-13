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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.description.Description;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationType;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for {@link KafkaDataSource}. */
public class KafkaDataSourceOptions {

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    /** The topic to consume from. */
    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required. Topic name(s) from which the data is read. "
                                    + "Multiple topics can be specified as a comma-separated list.");

    /** The bootstrap servers for the Kafka cluster. */
    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required. The Kafka bootstrap server list used to establish "
                                    + "the initial connection to the Kafka cluster.");

    /** The consumer group id for the Kafka consumer. */
    public static final ConfigOption<String> GROUP_ID =
            key("properties.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required. The consumer group id used to identify which "
                                    + "consumer group this source belongs to.");

    /** The format used to deserialize the value from Kafka. */
    public static final ConfigOption<JsonDeserializationType> VALUE_FORMAT =
            key("value.format")
                    .enumType(JsonDeserializationType.class)
                    .defaultValue(JsonDeserializationType.CANAL_JSON)
                    .withDescription(
                            "Defines the format identifier for decoding data from Kafka. "
                                    + "Currently only supports 'canal-json'.");

    /** The startup mode for the source. */
    public static final ConfigOption<StartupMode> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.LATEST_OFFSET)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Startup mode for Kafka consumer, valid values are "
                                                    + "'earliest-offset', 'latest-offset', "
                                                    + "'group-offsets', 'timestamp' and "
                                                    + "'specific-offsets'.")
                                    .build());

    /**
     * The specific offset to start consuming from. Only used when scan.startup.mode is
     * specific-offsets.
     */
    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies offsets for each partition in case the startup mode is "
                                    + "'specific-offsets'. "
                                    + "Example: 'partition:0,offset:42;partition:1,offset:300'.");

    /**
     * The timestamp in milliseconds to start consuming from. Only used when scan.startup.mode is
     * timestamp.
     */
    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Starts reading from the earliest record whose timestamp is greater "
                                    + "than or equal to the specified timestamp. "
                                    + "Only used when scan.startup.mode is 'timestamp'.");

    /**
     * The database name to use for table routing. If not set, will use the database from the
     * message.
     */
    public static final ConfigOption<String> DATABASE_NAME =
            key("database.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional. The default database name to use when the message does not "
                                    + "contain database information.");

    /**
     * The schema name to use for table routing. If not set, will use the schema from the message.
     */
    public static final ConfigOption<String> SCHEMA_NAME =
            key("schema.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional. The default schema name to use when the message does not "
                                    + "contain schema information.");

    /** The table name to use for table routing. If not set, will use the table from the message. */
    public static final ConfigOption<String> TABLE_NAME =
            key("table.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional. The default table name to use when the message does not "
                                    + "contain table information. When set, all records will be "
                                    + "routed to this table.");

    /** Whether to infer schema from the first message or use STRING for all columns. */
    public static final ConfigOption<Boolean> SCHEMA_INFER_ENABLED =
            key("schema.infer.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optional. If enabled, the schema will be inferred from the first "
                                    + "message. Otherwise, all columns will be treated as STRING. "
                                    + "Default is true.");
}
