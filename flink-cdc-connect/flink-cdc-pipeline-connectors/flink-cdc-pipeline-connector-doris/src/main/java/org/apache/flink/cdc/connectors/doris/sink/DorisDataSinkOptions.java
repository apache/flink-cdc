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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import org.apache.doris.flink.table.DorisConfigOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** DorisDataSink Options reference {@link DorisConfigOptions}. */
public class DorisDataSinkOptions {
    public static final ConfigOption<String> FENODES =
            ConfigOptions.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris fe http address.");
    public static final ConfigOption<String> BENODES =
            ConfigOptions.key("benodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris be http address.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris password.");
    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris jdbc url address.");
    public static final ConfigOption<Boolean> AUTO_REDIRECT =
            ConfigOptions.key("auto-redirect")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Use automatic redirection of fe without explicitly obtaining the be list");

    public static final ConfigOption<String> CHARSET_ENCODING =
            ConfigOptions.key("charset-encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Charset encoding for doris http client, default UTF-8.");

    // Streaming Sink options
    public static final ConfigOption<Boolean> SINK_ENABLE_2PC =
            ConfigOptions.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("enable 2PC while loading");

    public static final ConfigOption<Integer> SINK_CHECK_INTERVAL =
            ConfigOptions.key("sink.check-interval")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("check exception with the interval while loading");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<Integer> SINK_BUFFER_SIZE =
            ConfigOptions.key("sink.buffer-size")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription("the buffer size to cache data for stream load.");
    public static final ConfigOption<Integer> SINK_BUFFER_COUNT =
            ConfigOptions.key("sink.buffer-count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the buffer count to cache data for stream load.");
    public static final ConfigOption<String> SINK_LABEL_PREFIX =
            ConfigOptions.key("sink.label-prefix")
                    .stringType()
                    .defaultValue("")
                    .withDescription("the unique label prefix.");
    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE =
            ConfigOptions.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("whether to enable the delete function");

    // batch sink options
    public static final ConfigOption<Boolean> SINK_ENABLE_BATCH_MODE =
            ConfigOptions.key("sink.enable.batch-mode")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable batch write mode");

    public static final ConfigOption<Integer> SINK_FLUSH_QUEUE_SIZE =
            ConfigOptions.key("sink.flush.queue-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Queue length for async stream load, default is 2");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            "The maximum number of flush items in each batch, the default is 5w");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_BYTES =
            ConfigOptions.key("sink.buffer-flush.max-bytes")
                    .intType()
                    .defaultValue(10 * 1024 * 1024)
                    .withDescription(
                            "The maximum number of bytes flushed in each batch, the default is 10MB");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 10s.");

    public static final ConfigOption<Boolean> SINK_IGNORE_UPDATE_BEFORE =
            ConfigOptions.key("sink.ignore.update-before")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "In the CDC scenario, when the primary key of the upstream is inconsistent with that of the downstream, the update-before data needs to be passed to the downstream as deleted data, otherwise the data cannot be deleted.\n"
                                    + "The default is to ignore, that is, perform upsert semantics.");

    public static final ConfigOption<Boolean> SINK_USE_CACHE =
            ConfigOptions.key("sink.use-cache")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use buffer cache for breakpoint resume");

    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";
    // Prefix for Doris Create table.
    public static final String TABLE_CREATE_PROPERTIES_PREFIX = "table.create.properties.";
    // Prefix for Doris Create auto partition table.
    public static final String TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX =
            "table.create.auto-partition.properties.";
    public static final String TABLE_CREATE_PARTITION_KEY = "partition-key";
    public static final String TABLE_CREATE_PARTITION_UNIT = "partition-unit";

    public static final String TABLE_CREATE_DEFAULT_PARTITION_KEY =
            "default-" + TABLE_CREATE_PARTITION_KEY;
    public static final String TABLE_CREATE_DEFAULT_PARTITION_UNIT =
            "default-" + TABLE_CREATE_PARTITION_UNIT;

    public static final String TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY =
            TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX + TABLE_CREATE_DEFAULT_PARTITION_KEY;
    public static final String TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT =
            TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX + TABLE_CREATE_DEFAULT_PARTITION_UNIT;

    public static final String TABLE_CREATE_PARTITION_INCLUDE = "include";
    public static final String TABLE_CREATE_PARTITION_EXCLUDE = "exclude";

    public static final String TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE =
            TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX + TABLE_CREATE_PARTITION_INCLUDE;
    public static final String TABLE_CREATE_AUTO_PARTITION_PROPERTIES_EXCLUDE =
            TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX + TABLE_CREATE_PARTITION_EXCLUDE;

    public static Map<String, String> getPropertiesByPrefix(
            Configuration tableOptions, String prefix) {
        final Map<String, String> props = new HashMap<>();

        for (Map.Entry<String, String> entry : tableOptions.toMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String subKey = entry.getKey().substring(prefix.length());
                props.put(subKey, entry.getValue());
            }
        }
        return props;
    }
}
