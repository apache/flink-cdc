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

package org.apache.flink.cdc.connectors.mongodb.source.config;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SCHEME;

/** Configurations for {@link MongoDBSource}. */
public class MongoDBSourceOptions {

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

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the database to watch for changes.");

    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the collection in the database to watch for changes.");

    public static final ConfigOption<String> CONNECTION_OPTIONS =
            ConfigOptions.key("connection.options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ampersand-separated MongoDB connection options. "
                                    + "eg. replicaSet=test&connectTimeoutMS=300000");

    public static final ConfigOption<Integer> INITIAL_SNAPSHOTTING_QUEUE_SIZE =
            ConfigOptions.key("initial.snapshotting.queue.size")
                    .intType()
                    .noDefaultValue()
                    .withDeprecatedKeys("copy.existing.queue.size")
                    .withDescription(
                            "The max size of the queue to use when copying data. When not set,"
                                    + "it uses default value 16000 of mongo kafka connect sdk.");

    public static final ConfigOption<Integer> INITIAL_SNAPSHOTTING_MAX_THREADS =
            ConfigOptions.key("initial.snapshotting.max.threads")
                    .intType()
                    .noDefaultValue()
                    .withDeprecatedKeys("copy.existing.max.threads")
                    .withDescription(
                            "The number of threads to use when performing the data copy."
                                    + " Defaults to the number of processors.");

    public static final ConfigOption<String> INITIAL_SNAPSHOTTING_PIPELINE =
            ConfigOptions.key("initial.snapshotting.pipeline")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("copy.existing.pipeline")
                    .withDescription(
                            "An array of JSON objects describing the pipeline operations "
                                    + "to run when copying existing data. "
                                    + "This can improve the use of indexes by the copying manager and make copying more efficient.");

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
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable incremental snapshot. Defaults to false.");

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
}
