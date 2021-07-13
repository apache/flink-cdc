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

/** Configurations for {@link MySQLParallelSource}. */
public class MySQLSourceOptions {

    public static final String DATABASE_SERVER_NAME = "mysql_binlog_source";

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
                                    + "is like '5400,5408', The numeric ID range syntax is required when "
                                    + "'snapshot.parallel-read' enabled. Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL database cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<Boolean> SNAPSHOT_PARALLEL_SCAN =
            ConfigOptions.key("snapshot.parallel-scan")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable parallel scan snapshot of table or not, false by default."
                                    + "The 'server-id' is required to be a range syntax like '5400,5408'.");

    public static final ConfigOption<Integer> SCAN_SPLIT_SIZE =
            ConfigOptions.key("scan.split.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription("The split size used to cut splits for table.");
    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The fetch size for per poll.");

    public static final ConfigOption<String> SCAN_SPLIT_COLUMN =
            ConfigOptions.key("scan.split.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The single column used to cut splits for table,"
                                    + " the default value is primary key. If the primary key contains"
                                    + " multiple columns, this option is required to configure,"
                                    + " the configured column should make the splits as small as possible.");

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

    public static final ConfigOption<Boolean> SCAN_OPTIMIZE_INTEGRAL_KEY =
            ConfigOptions.key("scan.optimize.integral-key")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optimization to calculate the boundary of table snapshot split base on integral value rather than querying the DB,"
                                    + " by default this option is enabled.");

    // utils
    public static String validateAndGetServerId(ReadableConfig configuration) {
        final String serverIdValue = configuration.get(MySQLSourceOptions.SERVER_ID);
        // validate server id range
        if (configuration.get(MySQLSourceOptions.SNAPSHOT_PARALLEL_SCAN)) {
            String errMsg =
                    "The server id should be a range syntax like '5400,5404' when enable 'snapshot.parallel-scan' to 'true', "
                            + "but actual is %s";
            Preconditions.checkState(
                    serverIdValue != null
                            && serverIdValue.contains(",")
                            && serverIdValue.split(",").length == 2,
                    String.format(errMsg, serverIdValue));
            try {
                Integer.parseInt(serverIdValue.split(",")[0].trim());
                Integer.parseInt(serverIdValue.split(",")[1].trim());
            } catch (NumberFormatException e) {
                throw new IllegalStateException(String.format(errMsg, serverIdValue), e);
            }
        } else {
            // validate single server id
            try {
                if (serverIdValue != null) {
                    Integer.parseInt(serverIdValue);
                }
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                        String.format(
                                "The 'server.id' should contains single numeric ID, but is %s",
                                serverIdValue),
                        e);
            }
        }
        return serverIdValue;
    }

    public static int getServerId(String serverIdValue) {
        return Integer.parseInt(serverIdValue);
    }

    public static String getServerIdForSubTask(Configuration configuration, int subtaskId) {
        String serverIdRange = configuration.getString(MySQLSourceOptions.SERVER_ID);
        int serverIdStart = Integer.parseInt(serverIdRange.split(",")[0].trim());
        int serverIdEnd = Integer.parseInt(serverIdRange.split(",")[1].trim());
        int serverId = serverIdStart + subtaskId;
        Preconditions.checkState(
                serverIdStart <= serverId && serverId <= serverIdEnd,
                String.format(
                        "The server id %s in task %d is out of server id range %s, please keep the job parallelism same with server id num of server id range.",
                        serverId, subtaskId, serverIdRange));
        return String.valueOf(serverId);
    }
}
