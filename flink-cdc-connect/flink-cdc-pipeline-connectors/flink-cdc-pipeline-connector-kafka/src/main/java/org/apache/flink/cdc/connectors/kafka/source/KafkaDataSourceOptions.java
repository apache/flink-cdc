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
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for the Kafka pipeline source. */
public class KafkaDataSourceOptions {

    public static final String PROPERTIES_PREFIX = "properties.";

    public static final ConfigOption<String> TOPIC =
            key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Comma-separated Kafka topics to consume.");

    public static final ConfigOption<String> TOPIC_PATTERN =
            key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Regular expression matching Kafka topics to consume.");

    public static final ConfigOption<String> GROUP_ID =
            key("group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka consumer group id.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .stringType()
                    .defaultValue("group-offsets")
                    .withDescription(
                            "Startup mode. Supported values are earliest-offset, latest-offset, "
                                    + "group-offsets, timestamp, and specific-offsets.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode.");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Partition offsets used in case of \"specific-offsets\" startup mode. "
                                    + "Use 'partition:0,offset:42;partition:1,offset:300' when exactly one topic "
                                    + "is configured, or include a topic in each entry such as "
                                    + "'topic:dbz.customers,partition:0,offset:42'. Unspecified partitions "
                                    + "start from the earliest offset.");

    public static final ConfigOption<String> TABLES =
            key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional table inclusion patterns matched against Debezium source.db "
                                    + "and source.table. Regular expressions are supported. The dot (.) is "
                                    + "treated as a delimiter for database and table names. "
                                    + "eg. inventory.customers, inventory.user_table_[0-9]+");

    public static final ConfigOption<String> TABLES_EXCLUDE =
            key("tables.exclude")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional table exclusion patterns matched against Debezium source.db "
                                    + "and source.table. Regular expressions are supported. Can be used "
                                    + "alone or together with 'tables'.");

    public static final ConfigOption<JsonSerializationType> VALUE_FORMAT =
            key("value.format")
                    .enumType(JsonSerializationType.class)
                    .defaultValue(JsonSerializationType.DEBEZIUM_JSON)
                    .withDescription(
                            "Value format of Kafka records. Supported values are debezium-json "
                                    + "and canal-json. Default is debezium-json.");

    private KafkaDataSourceOptions() {}
}
