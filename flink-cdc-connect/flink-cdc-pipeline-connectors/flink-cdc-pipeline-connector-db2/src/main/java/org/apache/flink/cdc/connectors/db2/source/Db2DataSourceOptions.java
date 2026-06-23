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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link Db2DataSource}. */
@PublicEvolving
public class Db2DataSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the DB2 database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("Integer port number of the DB2 database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the DB2 user to use when connecting to the DB2 database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the DB2 database server.");

    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the DB2 tables to monitor. Regular expressions are supported. "
                                    + "The expected table name format is database.schema.table. "
                                    + "Note that the dot (.) is treated as a delimiter for database, schema, and table names. "
                                    + "To use a dot (.) in a regular expression to match any character, "
                                    + "escape the dot with a backslash. "
                                    + "For example: testdb.DB2INST1.\\.*, testdb.DB2INST1.user_table_[0-9]+.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "The session time zone of the database server. The default value is UTC.");

    public static final ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of the table snapshot. Captured tables are split into multiple chunks by a chunk key when reading the table snapshot. "
                                    + "By default, the chunk key is the first column of the primary key. "
                                    + "This column must be a column of the primary key.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of the table snapshot. Captured tables are split into multiple chunks when reading the table snapshot.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size per poll when reading a table snapshot.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the DB2 database server before timing out.");

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
                            "The maximum number of retries for building a DB2 database server connection.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for DB2 CDC consumer, valid enumerations are "
                                    + "\"initial\" or \"latest-offset\".");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP =
            ConfigOptions.key("scan.incremental.snapshot.backfill.skip")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to skip backfill in the snapshot reading phase. If backfill is skipped, changes on captured tables during the snapshot phase will be consumed later in the change log reading phase instead of being merged into the snapshot. WARNING: Skipping backfill might lead to data inconsistency because some change log events that happened within the snapshot phase might be replayed (only at-least-once semantics are promised). For example, this can happen when updating an already updated value in the snapshot or deleting an already deleted entry in the snapshot. These replayed change log events should be handled specially.");

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
                            "The upper bound of the chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table has an even distribution."
                                    + " The table chunks use the even distribution optimization when the data distribution is even,"
                                    + " and DB2 is queried for splitting when it is uneven."
                                    + " The distribution factor can be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of the chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table has an even distribution."
                                    + " The table chunks use the even distribution optimization when the data distribution is even,"
                                    + " and DB2 is queried for splitting when it is uneven."
                                    + " The distribution factor can be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase. This feature depends on "
                                    + "FLIP-147: Support Checkpoints After Tasks Finished. The Flink version must be "
                                    + "greater than or equal to 1.14 when enabling this feature.");

    @Experimental
    public static final ConfigOption<Boolean> SCHEMA_CHANGE_ENABLED =
            ConfigOptions.key("schema-change.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to send schema change events. The default value is true. If set to false, schema changes will not be sent.");

    @Experimental
    public static final ConfigOption<String> TABLES_EXCLUDE =
            ConfigOptions.key("tables.exclude")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the DB2 tables to exclude. Regular expressions are supported. "
                                    + "The expected table name format is database.schema.table. "
                                    + "Note that the dot (.) is treated as a delimiter for database, schema, and table names. "
                                    + "To use a dot (.) in a regular expression to match any character, "
                                    + "escape the dot with a backslash. "
                                    + "For example: testdb.DB2INST1.\\.*, testdb.DB2INST1.user_table_[0-9]+.");

    @Experimental
    public static final ConfigOption<String> METADATA_LIST =
            ConfigOptions.key("metadata.list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of readable metadata from SourceRecord to be passed downstream, split by `,`. "
                                    + "Available readable metadata fields are: database_name, schema_name, table_name, op_ts.");

    @Experimental
    public static final ConfigOption<Boolean>
            SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED =
                    ConfigOptions.key("scan.incremental.snapshot.unbounded-chunk-first.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Whether to assign the unbounded chunks first during the snapshot reading phase. This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk. Defaults to false.");
}
