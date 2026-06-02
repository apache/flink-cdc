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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Options for {@link PgVectorDataSink}. */
public class PgVectorDataSinkOptions {

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PostgreSQL JDBC URL.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PostgreSQL username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("PostgreSQL password.");

    public static final ConfigOption<String> DEFAULT_SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("Default target schema for table IDs without schema.");

    public static final ConfigOption<Boolean> CREATE_SCHEMA_ENABLED =
            ConfigOptions.key("create-schema.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create missing PostgreSQL schemas.");

    public static final ConfigOption<Boolean> CREATE_TABLE_ENABLED =
            ConfigOptions.key("create-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create missing PostgreSQL tables.");

    public static final ConfigOption<Boolean> CREATE_EXTENSION_ENABLED =
            ConfigOptions.key("create-extension.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to execute CREATE EXTENSION IF NOT EXISTS vector.");

    public static final ConfigOption<Integer> SINK_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.flush.max-rows")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Maximum rows to buffer before flushing JDBC batches.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Maximum interval before flushing JDBC batches.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retries for failed JDBC batch execution.");

    public static final ConfigOption<Duration> SINK_RETRY_BACKOFF =
            ConfigOptions.key("sink.retry.backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Backoff interval before retrying failed JDBC batches.");

    public static final ConfigOption<Boolean> SINK_DELETE_ENABLED =
            ConfigOptions.key("sink.delete.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to apply DELETE events.");

    public static final ConfigOption<Boolean> SINK_ALLOW_NO_PRIMARY_KEY =
            ConfigOptions.key("sink.allow-no-primary-key")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to allow append-only writes to tables without primary keys.");

    public static final String VECTOR_COLUMNS_PREFIX = "vector.columns.";
    public static final String TABLE_CREATE_PROPERTIES_PREFIX = "table.create.properties.";

    private PgVectorDataSinkOptions() {}

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
