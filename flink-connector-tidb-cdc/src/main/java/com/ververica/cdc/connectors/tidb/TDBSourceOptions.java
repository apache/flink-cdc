/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.tidb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;

import java.util.Map;

/** Configurations for {@link TiDBSource}. */
public class TDBSourceOptions {

    private TDBSourceOptions() {}

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the TiDB server to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the TiDB database to monitor.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for TiDB CDC consumer, valid enumerations are "
                                    + "\"initial\", \"latest-offset\"");

    public static final ConfigOption<String> PD_ADDRESSES =
            ConfigOptions.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiKV cluster's PD address");

    public static final ConfigOption<Long> TIKV_GRPC_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC timeout in ms");

    public static final ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC scan timeout in ms");

    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch get concurrency");

    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch scan concurrency");

    public static TiConfiguration getTiConfiguration(
            final String pdAddrsStr, final Map<String, String> options) {
        final Configuration configuration = Configuration.fromMap(options);

        final TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
        configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
        configuration
                .getOptional(TIKV_BATCH_GET_CONCURRENCY)
                .ifPresent(tiConf::setBatchGetConcurrency);

        configuration
                .getOptional(TIKV_BATCH_SCAN_CONCURRENCY)
                .ifPresent(tiConf::setBatchScanConcurrency);
        return tiConf;
    }
}
