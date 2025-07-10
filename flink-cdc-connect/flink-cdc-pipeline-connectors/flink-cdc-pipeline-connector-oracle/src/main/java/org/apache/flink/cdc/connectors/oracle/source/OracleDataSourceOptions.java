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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Configurations for {@link OracleDataSource}. */
@PublicEvolving
public class OracleDataSourceOptions {

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("jdbc.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The jdbc url.");
    public static final ConfigOption<String> SCHEMALIST =
            ConfigOptions.key("schemalist")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("schema list.");
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the oracle database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Integer port number of the oracle database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the oracle database to use when connecting to the oracle database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the oracle database server.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the oracle database server.");
    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the oracle tables to monitor. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<String> SERVER_ID =
            ConfigOptions.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408', The numeric ID range syntax is recommended when "
                                    + "'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all "
                                    + "currently-running database processes in the oracle cluster. This connector"
                                    + " joins the oracle  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
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
                            "The maximum time that the connector should wait after trying to connect to the oracle database server before timing out.");

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            ConfigOptions.key("connection.pool.size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The connection pool size.");

    public static final ConfigOption<Integer> CONNECT_MAX_RETRIES =
            ConfigOptions.key("connect.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times that the connector should retry to build oracle database server connection.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for oracle CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_FILE =
            ConfigOptions.key("scan.startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file name used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_POS =
            ConfigOptions.key("scan.startup.specific-offset.pos")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file position used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET =
            ConfigOptions.key("scan.startup.specific-offset.gtid-set")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional GTID set used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS =
            ConfigOptions.key("scan.startup.specific-offset.skip-events")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional number of events to skip after the specific starting offset");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS =
            ConfigOptions.key("scan.startup.specific-offset.skip-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional number of rows to skip after the specific offset");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available binlog offsets");

    // ----------------------------------------------------------------------------
    // experimental options, won't add them to documentation
    // ----------------------------------------------------------------------------
    @Experimental
    public static final ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query oracle for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query oracle for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase. This feature depends on "
                                    + "FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be "
                                    + "greater than or equal to 1.14 when enabling this feature.");

    @Experimental
    public static final ConfigOption<Boolean> SCHEMA_CHANGE_ENABLED =
            ConfigOptions.key("schema-change.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether send schema change events, by default is true. If set to false, the schema changes will not be sent.");

    @Experimental
    public static final ConfigOption<String> DATABASE_TABLE_CASE_INSENSITIVE =
            ConfigOptions.key("debezium.database.tablename.case.insensitive")
                    .stringType()
                    .defaultValue("false")
                    .withDescription(
                            "The case sensitivity of database and table names usually depends on the type of database and its configuration.");

    @Experimental
    public static final ConfigOption<String> DATABASE_CONNECTION_ADAPTER =
            ConfigOptions.key("debezium.database.connection.adapter")
                    .stringType()
                    .defaultValue("logminer")
                    .withDescription("Database connection adapter.");

    @Experimental
    public static final ConfigOption<String> LOG_MINING_STRATEGY =
            ConfigOptions.key("debezium.log.mining.strategy")
                    .stringType()
                    .defaultValue("online_catalog")
                    .withDescription("A strategy in log data analysis or mining.");

    @Experimental
    public static final ConfigOption<String> LOG_MINING_CONTINUOUS_MINE =
            ConfigOptions.key("debezium.log.mining.continuous.mine")
                    .stringType()
                    .defaultValue("true")
                    .withDescription(
                            "A continuous or ongoing mining process in the field of log mining.");

    @Experimental
    public static final ConfigOption<String> SNAPSHOT_LOCKING_MODE =
            ConfigOptions.key("debezium.snapshot.locking.mode")
                    .stringType()
                    .defaultValue("none")
                    .withDescription(
                            "Controls whether and for how long the connector holds a table lock. Table locks prevent certain types of changes table operations from occurring while the connector performs a snapshot. ");

    @Experimental
    public static final ConfigOption<String> HISTORY_CAPTURED_TABLES_DDL_ENABLE =
            ConfigOptions.key("debezium.database.history.store.only.captured.tables.ddl")
                    .stringType()
                    .defaultValue("true")
                    .withDescription(
                            "Used to specify whether the connector records schema structures from the schema or all tables in the database, or only from the tables specified for capture. ");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.\n"
                                    + "For Oracle, if you would like the source run in parallel, each parallel reader should have an unique server id, "
                                    + "so the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism.");

    public static final ConfigOption<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final ConfigOption<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP =
            ConfigOptions.key("scan.incremental.snapshot.backfill.skip")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to skip backfill in snapshot reading phase. If backfill is skipped, changes on captured tables during snapshot phase will be consumed later in binlog reading phase instead of being merged into the snapshot.WARNING: Skipping backfill might lead to data inconsistency because some binlog events happened within the snapshot phase might be replayed (only at-least-once semantic is promised). For example updating an already updated value in snapshot, or deleting an already deleted entry in snapshot. These replayed binlog events should be handled specially.");

    public static final ConfigOption<String> BIG_COLUMN_CHARACTER_SET =
            ConfigOptions.key("big.column.characterset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Character set encoding of large field data types at source side binary. ");

    @Experimental
    public static final ConfigOption<String> METADATA_LIST =
            ConfigOptions.key("metadata.list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of readable metadata from SourceRecord to be passed to downstream, split by `,`. "
                                    + "Available readable metadata are: op_ts.");

    public static Map<String, String> getPropertiesByPrefix(
            Configuration tableOptions, String prefix) {
        final Map<String, String> props = new HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.toMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String subKey = entry.getKey().substring(prefix.length());
                props.put(subKey, entry.getValue());
            }
        }
        return props;
    }
}
