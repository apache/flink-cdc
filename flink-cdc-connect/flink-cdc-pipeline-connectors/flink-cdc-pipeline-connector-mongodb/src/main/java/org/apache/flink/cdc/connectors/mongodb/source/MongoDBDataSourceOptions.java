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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SCHEME;
import static org.apache.flink.cdc.connectors.mongodb.source.SchemaParseMode.SCHEMA_LESS;

/** Configurations for {@link MongoDBSource}. */
public class MongoDBDataSourceOptions {

    public static final ConfigOption<String> SCHEME =
            ConfigOptions.key("scheme")
                    .stringType()
                    .defaultValue(MONGODB_SCHEME)
                    .withDescription(
                            "The protocol connected to MongoDB. eg. mongodb or mongodb+srv. "
                                    + "The +srv indicates to the client that the hostname that follows corresponds to a DNS SRV record. Defaults to mongodb.");

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of hostname and port pairs of the MongoDB servers. "
                                    + "eg. localhost:27017,localhost:27018");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database user to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the collection in the database to watch for changes.");

    @Experimental
    public static final ConfigOption<SchemaParseMode> SCHEMA_MODE =
            ConfigOptions.key("schema.mode")
                    .enumType(SchemaParseMode.class)
                    .defaultValue(SCHEMA_LESS)
                    .withDescription(
                            "MongoDB document schema parse mode, valid enumerations are SCHEMA_LESS, USER, SCHEMA_EVOLVE.");

    public static final ConfigOption<String> CONNECTION_OPTIONS =
            ConfigOptions.key("connection.options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ampersand-separated MongoDB connection options. "
                                    + "eg. replicaSet=test&connectTimeoutMS=300000");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The cursor batch size. Defaults to 1024.");

    public static final ConfigOption<Integer> POLL_MAX_BATCH_SIZE =
            ConfigOptions.key("poll.max.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Maximum number of change stream documents "
                                    + "to include in a single batch when polling for new data. "
                                    + "This setting can be used to limit the amount of data buffered internally in the connector. "
                                    + "Defaults to 1024.");

    public static final ConfigOption<Integer> POLL_AWAIT_TIME_MILLIS =
            ConfigOptions.key("poll.await.time.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The amount of time to wait before checking for new results on the change stream."
                                    + "Defaults: 1000.");

    public static final ConfigOption<Integer> HEARTBEAT_INTERVAL_MILLIS =
            ConfigOptions.key("heartbeat.interval.ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The length of time in milliseconds between sending heartbeat messages."
                                    + "Heartbeat messages contain the post batch resume token and are sent when no source records "
                                    + "have been published in the specified interval. This improves the resumability of the connector "
                                    + "for low volume namespaces. Use 0 to disable. Defaults to 0.");

    @Experimental
    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size.mb")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "The chunk size mb of incremental snapshot. Defaults to 64mb.");

    @Experimental
    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES =
            ConfigOptions.key("scan.incremental.snapshot.chunk.samples")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The number of samples to take per chunk. Defaults to 20.");

    @Experimental
    public static final ConfigOption<Boolean> FULL_DOCUMENT_PRE_POST_IMAGE =
            ConfigOptions.key("scan.full-changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Scan full mode changelog. Only available when MongoDB >= 6.0. Defaults to false.");

    public static final ConfigOption<Boolean> SCAN_NO_CURSOR_TIMEOUT =
            ConfigOptions.key("scan.cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for MySQL CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

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
    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to scan the newly added tables or not, by default is false. This option is only useful when we start the job from a savepoint/checkpoint.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_CHANGESTREAM_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.changestream.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "In changestream reading stage, whether to scan the ddl and dml statements of newly added tables or not, by default is false. \n"
                                    + "The difference between scan.newly-added-table.enabled and scan.binlog.newly-added-table.enabled options is: \n"
                                    + "scan.newly-added-table.enabled: do re-snapshot & changestream-reading for newly added table when restored; \n"
                                    + "scan.changestream.newly-added-table.enabled: only do changestream-reading for newly added table during changestream reading phase.");
}
