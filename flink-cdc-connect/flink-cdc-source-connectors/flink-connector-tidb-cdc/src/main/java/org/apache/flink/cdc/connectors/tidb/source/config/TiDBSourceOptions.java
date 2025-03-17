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
package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.tidb.utils.UriHostMapping;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.tikv.common.TiConfiguration;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class TiDBSourceOptions extends JdbcSourceOptions {

    public static final ConfigOption<Integer> TiDB_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(4000)
                    .withDescription("Integer port number of the TiDB database server.");

    public static final ConfigOption<String> PD_ADDRESSES =
            ConfigOptions.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiDB pd-server addresses");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available replication slot offsets");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of full names of tables, separated by commas, e.g. \"db1.table1, db2.table2\".");

    public static final ConfigOption<String> HOST_MAPPING =
            ConfigOptions.key("host-mapping")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TiKV cluster's host-mapping used to configure public IP and intranet IP mapping. When the TiKV cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Flink cluster to access. The format is {Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}, e.g. 192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9.");

    public static final ConfigOption<String> JDBC_DRIVER =
            ConfigOptions.key("jdbc.driver")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");

    public static TiConfiguration getTiConfiguration(
            final String pdAddrsStr, final String hostMapping, final Map<String, String> options) {
        final Configuration configuration = Configuration.fromMap(options);

        final TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        Optional.of(new UriHostMapping(hostMapping)).ifPresent(tiConf::setHostMapping);
        // todo add more config to tidb
        return tiConf;
    }
}
