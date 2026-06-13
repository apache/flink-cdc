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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Options for {@link LanceDbDataSink}. */
public class LanceDbDataSinkOptions {

    public static final ConfigOption<String> ROOT_PATH =
            ConfigOptions.key("root.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Root path used to store Lance datasets.");

    public static final ConfigOption<String> TABLE_PATH_MAPPING =
            ConfigOptions.key("table.path.mapping")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Source table to dataset path mapping, for example db.tbl:/tmp/db_tbl.lance;db.tbl2:s3://bucket/tbl2.lance.");

    public static final ConfigOption<Boolean> TABLE_NAME_NORMALIZE_ENABLED =
            ConfigOptions.key("table.name-normalize.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to normalize source table IDs when deriving dataset paths.");

    public static final ConfigOption<Boolean> CREATE_TABLE_ENABLED =
            ConfigOptions.key("create-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to create target Lance datasets if they do not exist.");

    public static final ConfigOption<Boolean> SCHEMA_VALIDATION_ENABLED =
            ConfigOptions.key("schema-validation.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to validate existing Lance dataset schema during CREATE_TABLE.");

    public static final ConfigOption<Boolean> SCHEMA_EVOLUTION_ENABLED =
            ConfigOptions.key("schema.evolution.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to apply compatible ADD_COLUMN schema changes.");

    public static final ConfigOption<String> SINK_CHANGELOG_MODE =
            ConfigOptions.key("sink.changelog-mode")
                    .stringType()
                    .defaultValue("append-only")
                    .withDescription(
                            "CDC changelog handling mode. Supported values are append-only, append-with-metadata, and reject.");

    public static final ConfigOption<Integer> SINK_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum rows to buffer before flushing Lance batches.");

    public static final ConfigOption<Integer> SINK_BATCH_MAX_ROWS_PER_COMMIT =
            ConfigOptions.key("sink.batch.max-rows-per-commit")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum rows to write in a single Lance commit.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Maximum interval before flushing Lance batches.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retries for failed Lance write batches.");

    public static final ConfigOption<Duration> SINK_RETRY_BACKOFF =
            ConfigOptions.key("sink.retry.backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500))
                    .withDescription("Backoff between Lance write retries.");

    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE =
            ConfigOptions.key("write.max-rows-per-file")
                    .intType()
                    .defaultValue(1_048_576)
                    .withDescription("Lance max rows per file write parameter.");

    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_GROUP =
            ConfigOptions.key("write.max-rows-per-group")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Lance max rows per row group write parameter.");

    public static final ConfigOption<Long> WRITE_MAX_BYTES_PER_FILE =
            ConfigOptions.key("write.max-bytes-per-file")
                    .longType()
                    .defaultValue(1_073_741_824L)
                    .withDescription("Lance max bytes per file write parameter.");

    public static final ConfigOption<Boolean> WRITE_ENABLE_STABLE_ROW_IDS =
            ConfigOptions.key("write.enable-stable-row-ids")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable stable row IDs in Lance write parameters.");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write.mode")
                    .stringType()
                    .defaultValue("CREATE")
                    .withDescription("Lance dataset creation write mode.");

    public static final ConfigOption<String> TIMEZONE =
            ConfigOptions.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("Timezone used when converting timestamp values.");

    public static final String STORAGE_OPTIONS_PREFIX = "storage.options.";

    private LanceDbDataSinkOptions() {}

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
