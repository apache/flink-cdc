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

package org.apache.flink.cdc.connectors.tidb;

import org.apache.flink.cdc.connectors.tidb.table.utils.UriHostMapping;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;

import java.util.Map;
import java.util.Optional;

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

    public static final ConfigOption<String> HOST_MAPPING =
            ConfigOptions.key("host-mapping")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TiKV cluster's host-mapping used to configure public IP and intranet IP mapping. When the TiKV cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Flink cluster to access. The format is {Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}, e.g. 192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9.");
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
            final String pdAddrsStr, final String hostMapping, final Map<String, String> options) {
        final Configuration configuration = Configuration.fromMap(options);

        final TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        Optional.of(new UriHostMapping(hostMapping)).ifPresent(tiConf::setHostMapping);
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
