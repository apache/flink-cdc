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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Options for {@link TDengineDataSink}. */
public class TDengineDataSinkOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TDengine WebSocket JDBC URL, for example jdbc:TAOS-WS://localhost:6041.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .defaultValue("root")
                    .withDescription("TDengine username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("taosdata")
                    .withDescription("TDengine password.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target TDengine database name.");

    public static final ConfigOption<String> STABLE_NAME =
            ConfigOptions.key("stable.name")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Default target TDengine super table name.");

    public static final ConfigOption<String> STABLE_MAPPING =
            ConfigOptions.key("stable.mapping")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Table to super table mapping, for example db.tbl:stable1;db.tbl2:stable2.");

    public static final ConfigOption<Boolean> NAME_NORMALIZE_ENABLED =
            ConfigOptions.key("name-normalize.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to normalize source table IDs to TDengine names.");

    public static final ConfigOption<String> TIMESTAMP_FIELD =
            ConfigOptions.key("timestamp.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Source field used as the TDengine primary timestamp column.");

    public static final ConfigOption<String> SUBTABLE_FIELD =
            ConfigOptions.key("subtable.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Source field used as the TDengine subtable name.");

    public static final ConfigOption<String> TAG_FIELDS =
            ConfigOptions.key("tag.fields")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Comma-separated source fields mapped to TDengine tags.");

    public static final ConfigOption<Boolean> CREATE_DATABASE_ENABLED =
            ConfigOptions.key("create-database.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create the target database if it does not exist.");

    public static final ConfigOption<Boolean> CREATE_STABLE_ENABLED =
            ConfigOptions.key("create-stable.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create target super tables if they do not exist.");

    public static final ConfigOption<Boolean> STABLE_SCHEMA_VALIDATION_ENABLED =
            ConfigOptions.key("stable-schema-validation.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to validate existing TDengine super table schema against the source schema during CREATE_TABLE.");

    public static final ConfigOption<Integer> VARCHAR_MAX_LENGTH_DEFAULT =
            ConfigOptions.key("varchar.max-length.default")
                    .intType()
                    .defaultValue(16374)
                    .withDescription("Default max length for TDengine VARCHAR/NCHAR fields.");

    public static final ConfigOption<String> STRING_TYPE =
            ConfigOptions.key("string.type")
                    .stringType()
                    .defaultValue("NCHAR")
                    .withDescription(
                            "TDengine string type for character columns: NCHAR or VARCHAR.");

    public static final ConfigOption<String> DECIMAL_MAPPING =
            ConfigOptions.key("decimal.mapping")
                    .stringType()
                    .defaultValue("DOUBLE")
                    .withDescription("TDengine mapping for DECIMAL columns: DOUBLE or NCHAR.");

    public static final ConfigOption<String> TIMEZONE =
            ConfigOptions.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("Timezone used when formatting timestamp values.");

    public static final ConfigOption<Integer> SINK_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum rows to buffer before flushing TDengine batches.");

    public static final ConfigOption<Integer> SINK_MAX_SQL_BYTES =
            ConfigOptions.key("sink.max-sql-bytes")
                    .intType()
                    .defaultValue(900_000)
                    .withDescription(
                            "Maximum rendered INSERT SQL size in bytes before splitting batches.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Maximum interval before flushing TDengine batches.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retries for failed TDengine write batches.");

    public static final ConfigOption<Duration> SINK_RETRY_BACKOFF =
            ConfigOptions.key("sink.retry.backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(200))
                    .withDescription("Backoff between TDengine write retries.");

    public static final ConfigOption<String> SINK_DELETE_MODE =
            ConfigOptions.key("sink.delete.mode")
                    .stringType()
                    .defaultValue("reject")
                    .withDescription(
                            "DELETE handling mode. Supported values are reject and ignore.");

    public static final ConfigOption<String> SINK_UPDATE_MODE =
            ConfigOptions.key("sink.update.mode")
                    .stringType()
                    .defaultValue("upsert")
                    .withDescription(
                            "UPDATE handling mode. Supported values are upsert and reject.");

    public static final ConfigOption<String> SINK_TIMESTAMP_CHANGE_MODE =
            ConfigOptions.key("sink.timestamp-change.mode")
                    .stringType()
                    .defaultValue("reject")
                    .withDescription(
                            "How to handle UPDATE events whose timestamp field changes. Supported values are reject and allow.");

    public static final ConfigOption<String> SINK_SUBTABLE_CHANGE_MODE =
            ConfigOptions.key("sink.subtable-change.mode")
                    .stringType()
                    .defaultValue("reject")
                    .withDescription(
                            "How to handle UPDATE events whose subtable field changes. Supported values are reject and allow.");

    public static final String CONNECTION_PROPERTIES_PREFIX = "connection.properties.";

    private TDengineDataSinkOptions() {}

    public static Map<String, String> getPropertiesByPrefix(
            Configuration configuration, String prefix) {
        Map<String, String> properties = new HashMap<>();
        configuration
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                properties.put(key.substring(prefix.length()), value);
                            }
                        });
        return properties;
    }
}
