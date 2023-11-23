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

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import org.apache.doris.flink.table.DorisConfigOptions;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

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
    public static final ConfigOption<String> TABLE_IDENTIFIER =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris table name.");
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

    // Streaming Sink options
    public static final ConfigOption<Boolean> SINK_ENABLE_2PC =
            ConfigOptions.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
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
                    .defaultValue(false)
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

    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";

    public static Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STREAM_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }
}
