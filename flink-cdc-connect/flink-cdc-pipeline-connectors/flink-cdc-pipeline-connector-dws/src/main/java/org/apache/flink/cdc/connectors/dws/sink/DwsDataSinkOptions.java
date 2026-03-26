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

import static com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions.DEF_CONNECTION_MAX_USE_TIME_SECONDS;

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
                    .withDescription(
                            Description.builder()
                                    .text("Write mode used by the Huawei DWS client.")
                                    .linebreak()
                                    .text(
                                            "Supported values include copy_merge, auto, upsert, copy_upsert, copy, update, update_auto, and copy_update.")
                                    .build());

    public static final ConfigOption<Integer> AUTO_BATCH_FLUSH_SIZE =
            ConfigOptions.key("auto-batch-flush-size")
                    .intType()
                    .defaultValue(50_000)
                    .withDescription("Maximum buffered rows before an automatic flush.");

    public static final ConfigOption<Duration> AUTO_FLUSH_MAX_INTERVAL =
            ConfigOptions.key("auto-flush-max-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription("Maximum interval between automatic flushes.");

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

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional database name override.");

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
                    .defaultValue(DEF_CONNECTION_MAX_USE_TIME_SECONDS)
                    .withDescription("Maximum lifetime of a connection in seconds.");

    public static final ConfigOption<Integer> CONNECTION_MAX_IDLE_MS =
            ConfigOptions.key("connectionMaxIdleMs")
                    .intType()
                    .defaultValue(60_000)
                    .withDescription("Maximum idle time for a connection in milliseconds.");

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
                    .withDescription("Whether automatic flush is enabled.");

    public static final ConfigOption<Boolean> ENABLE_DN_PARTITION =
            ConfigOptions.key("enable-dn-partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether Huawei DN partitioning is enabled.");

    public static final ConfigOption<String> DISTRIBUTION_KEY =
            ConfigOptions.key("distribution-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Distribution key used for DN partitioning.");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(5_000)
                    .withDescription("Maximum buffered rows before flushing.");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .intType()
                    .defaultValue(5 * 1024 * 1024)
                    .withDescription("Maximum buffered bytes before flushing.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(3))
                    .withDescription("Maximum time to buffer data before flushing.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries after a write failure.");

    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE =
            ConfigOptions.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether DELETE events are forwarded to the sink.");

    public static final ConfigOption<Boolean> SINK_ENABLE_UPSERT =
            ConfigOptions.key("sink.enable-upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether INSERT and UPDATE events use upsert semantics.");

    public static final ConfigOption<Integer> CONNECTION_POOL_MAX_SIZE =
            ConfigOptions.key("connection.pool.max-size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Maximum number of connections in the pool.");

    public static final ConfigOption<Integer> CONNECTION_POOL_MIN_SIZE =
            ConfigOptions.key("connection.pool.min-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Minimum number of connections kept in the pool.");

    public static final ConfigOption<Duration> CONNECTION_MAX_LIFETIME =
            ConfigOptions.key("connection.max-lifetime")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription("Maximum lifetime of a pooled connection.");

    public static final ConfigOption<Boolean> SINK_ENABLE_TABLE_CREATE =
            ConfigOptions.key("sink.enable.table-create")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether missing target tables may be created automatically.");

    public static final ConfigOption<String> TABLE_CREATE_PROPERTIES =
            ConfigOptions.key("table.create.properties")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Additional properties appended to CREATE TABLE statements.");
}
