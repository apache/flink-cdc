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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Options for {@link MilvusDataSink}. */
public class MilvusDataSinkOptions {

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus endpoint URI, for example http://localhost:19530.");

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Milvus token.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("Milvus client connect timeout.");

    public static final ConfigOption<Duration> RPC_DEADLINE =
            ConfigOptions.key("rpc.deadline")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Milvus RPC request deadline.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Target Milvus database name.");

    public static final ConfigOption<String> COLLECTION_MAPPING =
            ConfigOptions.key("collection.mapping")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Table to collection mapping, for example db.tbl:collection1;db.tbl2:collection2.");

    public static final ConfigOption<String> COLLECTION_COLLISION_CHECK_TABLES =
            ConfigOptions.key("collection.collision-check.tables")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Comma-separated source table IDs to validate normalized Milvus collection name collisions at startup.");

    public static final ConfigOption<Boolean> COLLECTION_NAME_NORMALIZE_ENABLED =
            ConfigOptions.key("collection.name-normalize.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to normalize table IDs into valid collection names.");

    public static final ConfigOption<Boolean> CREATE_COLLECTION_ENABLED =
            ConfigOptions.key("create-collection.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create missing Milvus collections.");

    public static final ConfigOption<Boolean> CREATE_INDEX_ENABLED =
            ConfigOptions.key("create-index.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to create vector indexes for new collections.");

    public static final ConfigOption<Boolean> ENABLE_DYNAMIC_FIELD =
            ConfigOptions.key("enable-dynamic-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable Milvus dynamic field for new collections.");

    public static final ConfigOption<Boolean> LOAD_COLLECTION_ENABLED =
            ConfigOptions.key("load-collection.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to load Milvus collection after schema/index setup.");

    public static final ConfigOption<String> PARTITION_FIELD =
            ConfigOptions.key("partition.field")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Optional source field used as Milvus partition name for writes.");

    public static final ConfigOption<Boolean> PARTITION_AUTO_CREATE_ENABLED =
            ConfigOptions.key("partition.auto-create.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to create missing Milvus partitions when partition.field is configured.");

    public static final ConfigOption<Integer> PARTITION_AUTO_CREATE_MAX_COUNT =
            ConfigOptions.key("partition.auto-create.max-count")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Maximum number of partitions a writer may auto-create at runtime.");

    public static final ConfigOption<String> PARTITION_NAMES =
            ConfigOptions.key("partition.names")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Comma-separated Milvus partition names to create during collection setup.");

    public static final ConfigOption<String> VECTOR_FIELDS =
            ConfigOptions.key("vector.fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Global vector field definitions, for example embedding:FloatVector(1536).");

    public static final ConfigOption<String> PRIMARY_KEY_FIELD =
            ConfigOptions.key("primary-key.field")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Explicit Milvus primary key field. If empty, source table single primary key is used.");

    public static final ConfigOption<Integer> VARCHAR_MAX_LENGTH_DEFAULT =
            ConfigOptions.key("varchar.max-length.default")
                    .intType()
                    .defaultValue(65535)
                    .withDescription("Default max length for Milvus VarChar fields.");

    public static final ConfigOption<Integer> SINK_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.flush.max-rows")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Maximum rows to buffer before flushing Milvus batches.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Maximum interval before flushing Milvus batches.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retries for failed Milvus write batches.");

    public static final ConfigOption<Duration> SINK_RETRY_BACKOFF =
            ConfigOptions.key("sink.retry.backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(200))
                    .withDescription("Backoff between Milvus write retries.");

    public static final ConfigOption<Boolean> SINK_ADAPTIVE_BATCH_SPLIT_ENABLED =
            ConfigOptions.key("sink.adaptive-batch-split.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to split write batches on Milvus rate limit or message size failures.");

    public static final ConfigOption<Integer> SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS =
            ConfigOptions.key("sink.adaptive-batch-split.min-rows")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Minimum batch size for adaptive split retries.");

    public static final ConfigOption<Boolean> SINK_DELETE_ENABLED =
            ConfigOptions.key("sink.delete.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to apply DELETE events.");

    public static final ConfigOption<String> SINK_PRIMARY_KEY_CHANGE_MODE =
            ConfigOptions.key("sink.primary-key-change.mode")
                    .stringType()
                    .defaultValue("reject")
                    .withDescription(
                            "How to handle UPDATE events whose primary key changes. Supported values are reject and allow.");

    public static final ConfigOption<Boolean> SINK_ALLOW_NO_PRIMARY_KEY =
            ConfigOptions.key("sink.allow-no-primary-key")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to allow append-only writes to collections without source primary keys.");

    public static final ConfigOption<String> CONSISTENCY_LEVEL =
            ConfigOptions.key("consistency-level")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Milvus consistency level for newly created collections. Empty means Milvus default.");

    public static final ConfigOption<String> INDEX_TYPE =
            ConfigOptions.key("index.type")
                    .stringType()
                    .defaultValue("AUTOINDEX")
                    .withDescription("Milvus vector index type.");

    public static final ConfigOption<String> INDEX_METRIC_TYPE =
            ConfigOptions.key("index.metric-type")
                    .stringType()
                    .defaultValue("COSINE")
                    .withDescription("Milvus vector index metric type.");

    public static final String VECTOR_FIELDS_TABLE_PREFIX = "vector.fields.";
    public static final String INDEX_PARAMS_PREFIX = "index.params.";

    private MilvusDataSinkOptions() {}

    public static Map<String, String> getPropertiesByPrefix(
            Configuration configuration, String prefix) {
        Map<String, String> properties = new HashMap<>();
        configuration
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                properties.put(key.substring(prefix.length()), value);
                            }
                        });
        return properties;
    }
}
