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

/** Configurations for {@link MySQLSource}. */
public class MySQLSourceOptions {

    private static final String IDENTIFIER = "mysql-cdc";

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

    public static final ConfigOption<Integer> SERVER_ID =
            ConfigOptions.key("server-id")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID of this database client, which must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector joins the "
                                    + "MySQL database cluster as another server (with this unique ID) so it can read the binlog. "
                                    + "By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<String> SERVER_ID_RANGE =
            ConfigOptions.key("server-id.range")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A server id range of the flink cdc connector to use when parallelism read the tables,"
                                    + " the syntax is \"startId, endId\", every source task will use one server is from the range."
                                    + "Every server id in the range is a numeric ID  of this database client, which must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector joins the "
                                    + "MySQL database cluster as another server (with this unique ID) so it can read the binlog. "
                                    + "By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for MySQL CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\"");

    public static final ConfigOption<Integer> SCAN_SPLIT_SIZE =
            ConfigOptions.key("scan.split.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription("The split size used to cut splits for table.");

    public static final ConfigOption<String> SCAN_SPLIT_COLUMN =
            ConfigOptions.key("scan.split.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The single column used to cut splits for table,"
                                    + " the default value is primary key. If the primary key contains"
                                    + " multiple columns, this option is required to configure,"
                                    + " the configured column should make the splits as small as possible.");

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The fetch size for per poll.");
}
