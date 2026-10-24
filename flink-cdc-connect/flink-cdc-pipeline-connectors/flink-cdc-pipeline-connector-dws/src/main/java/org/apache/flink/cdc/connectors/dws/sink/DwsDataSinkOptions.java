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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.description.Description;

import com.huaweicloud.dws.client.model.Constants;

import java.time.Duration;

/** Configuration options for the GaussDB DWS pipeline sink. */
public final class DwsDataSinkOptions {

    private DwsDataSinkOptions() {}

    public static final ConfigOption<String> URL =
            ConfigOptions.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GaussDB DWS JDBC URL.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GaussDB DWS username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GaussDB DWS password.");

    public static final ConfigOption<Boolean> CASE_SENSITIVE =
            ConfigOptions.key("case-sensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether quoted identifiers should preserve case.");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("dws.client.write.mode")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Native DWS client write mode. Supported values are auto, upsert, copy_upsert, and copy_merge.")
                                    .build());

    public static final ConfigOption<Integer> AUTO_BATCH_FLUSH_SIZE =
            ConfigOptions.key("auto-batch-flush-size")
                    .intType()
                    .defaultValue(30_000)
                    .withFallbackKeys("dws.client.write.auto-flush-size")
                    .withDescription("Native DWS client automatic flush batch size.");

    public static final ConfigOption<Duration> AUTO_FLUSH_MAX_INTERVAL =
            ConfigOptions.key("auto-flush-max-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(3))
                    .withFallbackKeys("dws.client.write.auto-flush-max-interval")
                    .withDescription("Native DWS client automatic flush interval.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("sink-table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional sink table override.");

    public static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .defaultValue("com.huawei.gauss200.jdbc.Driver")
                    .withDescription("GaussDB DWS JDBC driver class.");

    public static final ConfigOption<Boolean> LOG_SWITCH =
            ConfigOptions.key("logSwitch")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable DWS client logging.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom sink parallelism. If unset, the planner derives it automatically.");

    public static final ConfigOption<String> PIPELINE_LOCAL_TIME_ZONE =
            ConfigOptions.key("local-time-zone")
                    .stringType()
                    .defaultValue("systemDefault")
                    .withDescription(
                            Description.builder()
                                    .text("Session time zone used for timestamp conversion.")
                                    .linebreak()
                                    .text(
                                            "Accepts full time zone IDs such as \"America/Los_Angeles\" or custom offsets such as \"GMT-08:00\".")
                                    .build());

    public static final ConfigOption<Integer> CONNECTION_SIZE =
            ConfigOptions.key("connectionSize")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Number of connections used by the DWS client.");

    public static final ConfigOption<Integer> CONNECTION_MAX_USE_TIME_SECONDS =
            ConfigOptions.key("connectionMaxUseTimeSeconds")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("Maximum lifetime of a connection in seconds.");

    public static final ConfigOption<Integer> CONNECTION_MAX_USE_TIME_THRESHOLD =
            ConfigOptions.key("connectionMaxUseTimeThreshold")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Compatibility connection lifetime in seconds.");

    public static final ConfigOption<Duration> DWS_CLIENT_JDBC_MAX_USE_TIME =
            ConfigOptions.key("dws.client.jdbc.max.use-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Native DWS connection lifetime.");

    public static final ConfigOption<Integer> CONNECTION_MAX_IDLE_MS =
            ConfigOptions.key("connectionMaxIdleMs")
                    .intType()
                    .defaultValue(60_000)
                    .withDescription("Maximum idle time for a connection in milliseconds.");

    public static final ConfigOption<Duration> DWS_CLIENT_JDBC_MAX_IDLE =
            ConfigOptions.key("dws.client.jdbc.max.idle")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Native DWS connection idle duration.");

    public static final ConfigOption<Long> CONNECTION_TIME_OUT =
            ConfigOptions.key("connectionTimeOut")
                    .longType()
                    .defaultValue(Constants.CONNECTION_TIME_OUT)
                    .withDescription("Connection timeout in milliseconds.");

    public static final ConfigOption<String> CONNECTION_POOL_NAME =
            ConfigOptions.key("connectionPoolName")
                    .stringType()
                    .defaultValue(Constants.CONNECTION_POOL_NAME)
                    .withDescription("Connection pool name.");

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            ConfigOptions.key("connectionPoolSize")
                    .intType()
                    .defaultValue(Constants.CONNECTION_POOL_SIZE)
                    .withDescription("Connection pool size.");

    public static final ConfigOption<Long> CONNECTION_POOL_TIMEOUT =
            ConfigOptions.key("connectionPoolTimeout")
                    .longType()
                    .defaultValue(Constants.CONNECTION_POOL_TIMEOUT)
                    .withDescription("Connection pool timeout in milliseconds.");

    public static final ConfigOption<Integer> CONNECTION_SOCKET_TIMEOUT =
            ConfigOptions.key("connectionSocketTimeout")
                    .intType()
                    .defaultValue(Constants.CONNECTION_SOCKET_TIMEOUT)
                    .withDescription("Socket timeout in milliseconds.");

    public static final ConfigOption<Long> CONNECTION_MAX_USE_COUNT =
            ConfigOptions.key("connectionMaxUseCount")
                    .longType()
                    .defaultValue(Constants.CONNECTION_MAX_USE_COUNT)
                    .withDescription("Maximum number of operations per connection.");

    public static final ConfigOption<Boolean> NEED_CONNECTION_POOL_MONITOR =
            ConfigOptions.key("needConnectionPoolMonitor")
                    .booleanType()
                    .defaultValue(Constants.NEED_CONNECTION_POOL_MONITOR)
                    .withDescription("Whether to enable connection pool monitoring.");

    public static final ConfigOption<Long> CONNECTION_POOL_MONITOR_PERIOD =
            ConfigOptions.key("connectionPoolMonitorPeriod")
                    .longType()
                    .defaultValue(Constants.CONNECTION_POOL_MONITOR_PERIOD)
                    .withDescription("Connection pool monitor interval in milliseconds.");

    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("Default schema name.");

    public static final ConfigOption<Boolean> ENABLE_AUTO_FLUSH =
            ConfigOptions.key("enable-auto-flush")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether native batch and interval auto flush are enabled.");

    public static final ConfigOption<Boolean> ENABLE_DN_PARTITION =
            ConfigOptions.key("enable-dn-partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether Huawei DN partitioning is enabled.");

    public static final ConfigOption<Integer> DWS_CLIENT_WRITE_THREAD_SIZE =
            ConfigOptions.key("dws.client.write.thread-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Native DWS client write worker thread count.");

    public static final ConfigOption<Integer> DWS_CLIENT_WRITE_USE_COPY_SIZE =
            ConfigOptions.key("dws.client.write.use-copy-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Native DWS client AUTO-to-COPY threshold.");

    public static final ConfigOption<Integer> DWS_CLIENT_WRITE_FORCE_FLUSH_SIZE =
            ConfigOptions.key("dws.client.write.force-flush-size")
                    .intType()
                    .defaultValue(40000)
                    .withDescription("Finite native DWS client force flush threshold.");

    public static final ConfigOption<String> DISTRIBUTION_KEY =
            ConfigOptions.key("distribution-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Distribution key used for DN partitioning.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys("dws.client.retry.max-times")
                    .withDescription("Native DWS client total attempt count.");

    public static final ConfigOption<Duration> DWS_CLIENT_RETRY_SLEEP_BASE_TIME =
            ConfigOptions.key("dws.client.retry.sleep-base-time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Base delay between native DWS client attempts.");

    public static final ConfigOption<Duration> DWS_CLIENT_RETRY_SLEEP_RANDOM_TIME =
            ConfigOptions.key("dws.client.retry.sleep-random-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(300))
                    .withDescription("Positive random retry jitter for the native DWS client.");

    public static final ConfigOption<Duration> DWS_CLIENT_TIMEOUT_TASK =
            ConfigOptions.key("dws.client.timeout.task")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("Finite native task timeout; not a whole-flush deadline.");

    public static final ConfigOption<Duration> DWS_CLIENT_TIMEOUT_STATEMENT =
            ConfigOptions.key("dws.client.timeout.statement")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("DWS statement timeout.");

    public static final ConfigOption<String> DWS_CLIENT_BUFFER_ALL_MAX_BYTES =
            ConfigOptions.key("dws.client.write.buffer.all-max-bytes")
                    .stringType()
                    .defaultValue("128MiB")
                    .withDescription("Estimated total native buffer budget per sink writer.");

    public static final ConfigOption<String> DWS_CLIENT_BUFFER_TABLE_MAX_BYTES =
            ConfigOptions.key("dws.client.write.buffer.table-max-bytes")
                    .stringType()
                    .defaultValue("64MiB")
                    .withDescription("Estimated native buffer budget per table and sink writer.");

    public static final ConfigOption<String> DWS_CLIENT_BUFFER_PARTITION_MAX_BYTES =
            ConfigOptions.key("dws.client.write.buffer.partition-max-bytes")
                    .stringType()
                    .defaultValue("32MiB")
                    .withDescription("Estimated native buffer budget for the fixed partition.");

    public static final ConfigOption<String> DWS_CLIENT_WRITE_PARTITION_POLICY =
            ConfigOptions.key("dws.client.write.partition-policy")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Reserved native partition policy; connector-fixed.");

    public static final ConfigOption<Integer> DWS_CLIENT_WRITE_PARTITION_MIN =
            ConfigOptions.key("dws.client.write.partition-min")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Reserved native minimum partition count; connector-fixed.");

    public static final ConfigOption<Integer> DWS_CLIENT_WRITE_PARTITION_MAX =
            ConfigOptions.key("dws.client.write.partition-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Reserved native maximum partition count; connector-fixed.");

    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE =
            ConfigOptions.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether DELETE events are forwarded to the sink.");
}
