/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Optional;

/** Configurations for {@link MySqlParallelSource}. */
public class MySqlSourceOptions {

    public static final String DATABASE_SERVER_NAME = "mysql_binlog_source";
    public static final String DATABASE_SERVER_ID = "database.server.id";

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the MySQL database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Integer port number of the MySQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the MySQL database to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the MySQL server to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the MySQL database to monitor.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<String> SERVER_ID =
            ConfigOptions.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408', The numeric ID range syntax is recommended when "
                                    + "'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<Boolean> SCAN_SNAPSHOT_PARALLEL_READ =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.\n"
                                    + "If you would like the source run in parallel, each parallel reader should have an unique server id, "
                                    + "so the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the MySQL database server before timing out.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for MySQL CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_FILE =
            ConfigOptions.key("scan.startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Integer> SCAN_STARTUP_SPECIFIC_OFFSET_POS =
            ConfigOptions.key("scan.startup.specific-offset.pos")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    // utils
    public static String validateAndGetServerId(ReadableConfig configuration) {
        final String serverIdValue = configuration.get(MySqlSourceOptions.SERVER_ID);
        if (serverIdValue != null) {
            if (serverIdValue.contains("-")) {
                String errMsg =
                        "The '%s' should be a range syntax like '5400-5404' when enable '%s', "
                                + "but actual is %s";
                Preconditions.checkState(
                        serverIdValue.split("-").length == 2,
                        String.format(
                                errMsg,
                                SERVER_ID.key(),
                                SCAN_SNAPSHOT_PARALLEL_READ.key(),
                                serverIdValue));
                checkServerId(serverIdValue.split("-")[0].trim());
                checkServerId(serverIdValue.split("-")[1].trim());
            } else {
                checkServerId(serverIdValue);
            }
        }
        return serverIdValue;
    }

    private static void checkServerId(String serverIdValue) {
        try {
            Integer.parseInt(serverIdValue);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format(
                            "The 'server-id' should contains single numeric ID like '5400' or numeric ID range '5400-5404', but actual is %s",
                            serverIdValue),
                    e);
        }
    }

    public static int getServerId(String serverIdValue) {
        return Integer.parseInt(serverIdValue);
    }

    public static Optional<String> getServerIdForSubTask(
            Configuration configuration, int subtaskId) {
        String serverIdRange = configuration.getString(MySqlSourceOptions.SERVER_ID);
        if (serverIdRange == null) {
            return Optional.empty();
        }
        if (serverIdRange.contains("-")) {
            int serverIdStart = Integer.parseInt(serverIdRange.split("-")[0].trim());
            int serverIdEnd = Integer.parseInt(serverIdRange.split("-")[1].trim());
            int serverId = serverIdStart + subtaskId;
            Preconditions.checkState(
                    serverIdStart <= serverId && serverId <= serverIdEnd,
                    String.format(
                            "The server id %s in task %d is out of server id range %s, please keep the job parallelism same with server id num of server id range.",
                            serverId, subtaskId, serverIdRange));
            return Optional.of(String.valueOf(serverId));
        } else {
            int serverIdStart = Integer.parseInt(serverIdRange);
            if (subtaskId > 0) {
                throw new IllegalStateException(
                        String.format(
                                "The server id should a range like '5400-5404' when %s enabled , but actual is %s",
                                SCAN_SNAPSHOT_PARALLEL_READ.key(), serverIdRange));
            } else {
                return Optional.of(String.valueOf(serverIdStart));
            }
        }
    }
}
