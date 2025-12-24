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

package org.apache.flink.cdc.connectors.gaussdb.source.config;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for GaussDB source. */
public class GaussDBSourceOptions extends JdbcSourceOptions {

        // ------------------------------------------------------------------------------------------
        // Required options
        // ------------------------------------------------------------------------------------------

        public static final ConfigOption<String> HOSTNAME = JdbcSourceOptions.HOSTNAME;

        public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
                        .intType()
                        .defaultValue(8000)
                        .withDescription("Integer port number of the GaussDB database server.");

        /** @deprecated Use {@link #PORT} instead. */
        @Deprecated
        public static final ConfigOption<Integer> GAUSSDB_PORT = PORT;

        public static final ConfigOption<Integer> HA_PORT = ConfigOptions.key("ha-port")
                        .intType()
                        .noDefaultValue()
                        .withDescription("Optional HA port number for GaussDB logical replication.");

        public static final ConfigOption<String> USERNAME = JdbcSourceOptions.USERNAME;

        public static final ConfigOption<String> PASSWORD = JdbcSourceOptions.PASSWORD;

        public static final ConfigOption<String> DATABASE_NAME = JdbcSourceOptions.DATABASE_NAME;

        public static final ConfigOption<String> SLOT_NAME = ConfigOptions.key("slot.name")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                        "The name of the GaussDB logical decoding slot that was created for "
                                                        + "streaming changes from a particular plug-in for a "
                                                        + "particular database/schema. The server uses this slot to "
                                                        + "stream events to the connector that you are configuring.");

        // ------------------------------------------------------------------------------------------
        // Optional options
        // ------------------------------------------------------------------------------------------

        public static final ConfigOption<String> SCHEMA_NAME = ConfigOptions.key("schema-name")
                        .stringType()
                        .defaultValue("public")
                        .withDescription("Schema name of the GaussDB database to monitor.");

        public static final ConfigOption<String> DECODING_PLUGIN_NAME = ConfigOptions.key("decoding.plugin.name")
                        .stringType()
                        .defaultValue("mppdb_decoding")
                        .withDescription(
                                        "The name of the GaussDB logical decoding plug-in installed on the "
                                                        + "server.");

        // ------------------------------------------------------------------------------------------
        // Scan options
        // ------------------------------------------------------------------------------------------

        public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED = ConfigOptions
                        .key("scan.incremental.snapshot.enabled")
                        .booleanType()
                        .defaultValue(true) // align with SourceOptions default
                        .withDescription(
                                        "Whether to enable parallel incremental snapshot. If enabled, the "
                                                        + "source can be parallel during snapshot reading and perform "
                                                        + "checkpoints in chunk granularity.");

        public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED = ConfigOptions
                        .key("scan.newly-added-table.enabled")
                        .booleanType()
                        .defaultValue(false) // false allows stream events to pass through
                        .withDescription(
                                        "Whether to scan newly added tables. When set to false, "
                                                        + "stream events from tables not tracked in finishedSplitsInfo "
                                                        + "will be allowed to flow through.");

        public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE = SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;

        public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions.key("scan.startup.mode")
                        .stringType()
                        .defaultValue("initial")
                        .withDescription(
                                        "Optional startup mode for GaussDB CDC consumer, valid enumerations are "
                                                        + "\"initial\", \"snapshot\", \"latest-offset\".\n"
                                                        + "\"initial\": Performs an initial snapshot on the monitored database tables upon first startup, "
                                                        + "and continue to read the latest changes.\n"
                                                        + "\"snapshot\": Performs a snapshot on the monitored database tables and exits.\n"
                                                        + "\"latest-offset\": Never to perform snapshot on the monitored database tables upon first startup, "
                                                        + "just read from the latest offset (requires 'scan.incremental.snapshot.enabled' = 'true').");

        // ------------------------------------------------------------------------------------------
        // Streaming options
        // ------------------------------------------------------------------------------------------

        public static final ConfigOption<Duration> HEARTBEAT_INTERVAL = ConfigOptions.key("heartbeat.interval.ms")
                        .durationType()
                        .defaultValue(Duration.ofSeconds(30))
                        .withDescription(
                                        "Optional interval of sending heartbeat event for tracing the latest "
                                                        + "available replication slot offsets.");

        public static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE = ConfigOptions.key("changelog-mode")
                        .enumType(DebeziumChangelogMode.class)
                        .defaultValue(DebeziumChangelogMode.ALL)
                        .withDescription(
                                        "The changelog mode used for encoding streaming changes.\n"
                                                        + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                                        + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key. It can be used for tables with primary keys when replica identity FULL is not an option.");
}
