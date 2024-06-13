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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;

import java.time.Duration;

/** Options for {@link OceanBaseDataSink}. */
public class OceanBaseDataSinkOptions {
    // ------------------------------------------------------------------------------------------
    // Options for sink connector
    // ------------------------------------------------------------------------------------------
    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The connection URL.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password.");

    public static final ConfigOption<String> DRIVER_CLASS_NAME =
            ConfigOptions.key("driver-class-name")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");

    public static final ConfigOption<String> DRUID_PROPERTIES =
            ConfigOptions.key("druid-properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties for specific connection pool.");

    public static final ConfigOption<Boolean> MEMSTORE_CHECK_ENABLED =
            ConfigOptions.key("memstore-check.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether enable memstore check. Default value is 'true'");

    public static final ConfigOption<Double> MEMSTORE_THRESHOLD =
            ConfigOptions.key("memstore-check.threshold")
                    .doubleType()
                    .defaultValue(0.9)
                    .withDescription(
                            "Memory usage threshold ratio relative to the limit value. Default value is '0.9'.");

    public static final ConfigOption<Duration> MEMSTORE_CHECK_INTERVAL =
            ConfigOptions.key("memstore-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The check interval, over this time, the writer will check if memstore reaches threshold. Default value is '30s'.");

    public static final ConfigOption<Boolean> PARTITION_ENABLED =
            ConfigOptions.key("partition.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable partition calculation and flush records by partitions. Default value is 'false'.");

    @Experimental
    public static final ConfigOption<Boolean> DIRECT_LOAD_ENABLED =
            ConfigOptions.key("direct-load.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable direct load.");

    @Experimental
    public static final ConfigOption<String> DIRECT_LOAD_HOST =
            ConfigOptions.key("direct-load.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname used in direct load.");

    @Experimental
    public static final ConfigOption<Integer> DIRECT_LOAD_PORT =
            ConfigOptions.key("direct-load.port")
                    .intType()
                    .defaultValue(2882)
                    .withDescription("Rpc port number used in direct load.");

    @Experimental
    public static final ConfigOption<Integer> DIRECT_LOAD_PARALLEL =
            ConfigOptions.key("direct-load.parallel")
                    .intType()
                    .defaultValue(8)
                    .withDescription("Parallelism of direct load.");

    @Experimental
    public static final ConfigOption<Long> DIRECT_LOAD_MAX_ERROR_ROWS =
            ConfigOptions.key("direct-load.max-error-rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Maximum tolerable number of error rows.");

    @Experimental
    public static final ConfigOption<ObLoadDupActionType> DIRECT_LOAD_DUP_ACTION =
            ConfigOptions.key("direct-load.dup-action")
                    .enumType(ObLoadDupActionType.class)
                    .defaultValue(ObLoadDupActionType.REPLACE)
                    .withDescription("Action when there is duplicated record in direct load.");

    @Experimental
    public static final ConfigOption<Duration> DIRECT_LOAD_TIMEOUT =
            ConfigOptions.key("direct-load.timeout")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription("Timeout for direct load task.");

    @Experimental
    public static final ConfigOption<Duration> DIRECT_LOAD_HEARTBEAT_TIMEOUT =
            ConfigOptions.key("direct-load.heartbeat-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Client heartbeat timeout in direct load task.");

    public static final ConfigOption<Boolean> SYNC_WRITE =
            ConfigOptions.key("sync-write")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to write synchronously.");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval, over this time, asynchronous threads will flush data. Default value is '1s'. "
                                    + "If it's set to zero value like '0', scheduled flushing will be disabled.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-flush.buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Buffer size. Default value is '1000'.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times if writing records to database failed. Default value is '3'.");
}
