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

package org.apache.flink.cdc.connectors.fluss.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.source.discover.TableDiscovererFactory;

import java.time.Duration;

/** Options for Fluss DataSource. */
public class FlussDataSourceOptions {

    public static final String CLIENT_PROPERTIES_PREFIX = "properties.client.";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The bootstrap servers for the Fluss source connection.");

    /**
     * Prefix shared by all {@code table.discoverer.*} keys. {@code FlussDataSourceFactory} excludes
     * this prefix from strict option validation so that each {@link TableDiscovererFactory}
     * implementation is free to declare its own namespaced options (e.g. {@code
     * table.discoverer.pattern}, {@code table.discoverer.jdbc.url}) without being enumerated here.
     */
    public static final String TABLE_DISCOVERER_OPTIONS_PREFIX = "table.discoverer.";

    public static final ConfigOption<String> TABLE_DISCOVERER_TYPE =
            ConfigOptions.key("table.discoverer.type")
                    .stringType()
                    .defaultValue("fluss-default")
                    .withDescription(
                            "The discoverer type that decides which tables to read. The value "
                                    + "is matched against the identifier of a registered "
                                    + "TableDiscovererFactory (loaded via Java SPI). Built-in values: "
                                    + "'fluss-default' (default; reads its config from "
                                    + "'table.discoverer.pattern') and 'jdbc' (reads its config from "
                                    + "'table.discoverer.jdbc.url', etc.).");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription(
                            "The startup mode for the Fluss source. "
                                    + "Supported values are 'earliest', 'latest', 'full', and 'timestamp'. "
                                    + "'full' performs a full snapshot on the table upon first startup, "
                                    + "then continues to read the log. "
                                    + "When 'timestamp' is used, 'scan.startup.timestamp' must also be set.");

    public static final ConfigOption<String> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The startup timestamp (milliseconds or 'yyyy-MM-dd HH:mm:ss' format) "
                                    + "for the Fluss source when scan.startup.mode is 'timestamp'.");

    public static final ConfigOption<Duration> SCAN_DISCOVERY_INTERVAL =
            ConfigOptions.key("scan.discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The time interval for the Fluss source to discover "
                                    + "new tables, partitions, and buckets while scanning. "
                                    + "A non-positive value disables the periodic discovery.");

    public static final ConfigOption<String> SCAN_KV_SNAPSHOT_LEASE_ID =
            ConfigOptions.key("scan.kv.snapshot.lease.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The lease ID used to protect acquired KV snapshots from deletion. "
                                    + "A random UUID is used by default.");

    public static final ConfigOption<Duration> SCAN_KV_SNAPSHOT_LEASE_DURATION =
            ConfigOptions.key("scan.kv.snapshot.lease.duration")
                    .durationType()
                    .defaultValue(Duration.ofDays(1))
                    .withDescription(
                            "The duration for retaining KV snapshots acquired by the source.");

    private FlussDataSourceOptions() {}
}
