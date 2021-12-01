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

package com.ververica.cdc.connectors.mongodb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;

import com.mongodb.ConnectionString;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.ErrorTolerance;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBConnectorSourceConnector;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import io.debezium.heartbeat.Heartbeat;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume change stream
 * events.
 */
@PublicEvolving
public class MongoDBSource {

    public static final String MONGODB_SCHEME = "mongodb";

    public static final String ERROR_TOLERANCE_NONE = ErrorTolerance.NONE.value();

    public static final String ERROR_TOLERANCE_ALL = ErrorTolerance.ALL.value();

    public static final String FULL_DOCUMENT_UPDATE_LOOKUP = FullDocument.UPDATE_LOOKUP.getValue();

    public static final int POLL_MAX_BATCH_SIZE_DEFAULT = 1000;

    public static final int POLL_AWAIT_TIME_MILLIS_DEFAULT = 1500;

    public static final String HEARTBEAT_TOPIC_NAME_DEFAULT = "__mongodb_heartbeats";

    public static final String OUTPUT_FORMAT_SCHEMA =
            OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT);

    // Add "source" field to adapt to debezium SourceRecord
    public static final String OUTPUT_SCHEMA_VALUE_DEFAULT =
            "{"
                    + "  \"name\": \"ChangeStream\","
                    + "  \"type\": \"record\","
                    + "  \"fields\": ["
                    + "    { \"name\": \"_id\", \"type\": \"string\" },"
                    + "    { \"name\": \"operationType\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"fullDocument\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"source\","
                    + "      \"type\": [{\"name\": \"source\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"ts_ms\", \"type\": \"long\"},"
                    + "                {\"name\": \"snapshot\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"ns\","
                    + "      \"type\": [{\"name\": \"ns\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"to\","
                    + "      \"type\": [{\"name\": \"to\", \"type\": \"record\",  \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"documentKey\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"updateDescription\","
                    + "      \"type\": [{\"name\": \"updateDescription\",  \"type\": \"record\", \"fields\": ["
                    + "                 {\"name\": \"updatedFields\", \"type\": [\"string\", \"null\"]},"
                    + "                 {\"name\": \"removedFields\","
                    + "                  \"type\": [{\"type\": \"array\", \"items\": \"string\"}, \"null\"]"
                    + "                  }] }, \"null\"] },"
                    + "    { \"name\": \"clusterTime\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"txnNumber\", \"type\": [\"long\", \"null\"]},"
                    + "    { \"name\": \"lsid\", \"type\": [{\"name\": \"lsid\", \"type\": \"record\","
                    + "               \"fields\": [ {\"name\": \"id\", \"type\": \"string\"},"
                    + "                             {\"name\": \"uid\", \"type\": \"string\"}] }, \"null\"] }"
                    + "  ]"
                    + "}";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /** Builder class of {@link MongoDBSource}. */
    public static class Builder<T> {

        private String hosts;
        private String username;
        private String password;
        private String database;
        private String collection;
        private String connectionOptions;
        private String pipeline;
        private Integer batchSize;
        private Integer pollAwaitTimeMillis = POLL_AWAIT_TIME_MILLIS_DEFAULT;
        private Integer pollMaxBatchSize = POLL_MAX_BATCH_SIZE_DEFAULT;
        private Boolean copyExisting = true;
        private Integer copyExistingMaxThreads;
        private Integer copyExistingQueueSize;
        private String copyExistingPipeline;
        private Boolean errorsLogEnable;
        private String errorsTolerance;
        private Integer heartbeatIntervalMillis;
        private DebeziumDeserializationSchema<T> deserializer;

        /** The comma-separated list of hostname and port pairs of mongodb servers. */
        public Builder<T> hosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        /**
         * Ampersand (i.e. &) separated MongoDB connection options eg
         * replicaSet=test&connectTimeoutMS=300000
         * https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options
         */
        public Builder<T> connectionOptions(String connectionOptions) {
            this.connectionOptions = connectionOptions;
            return this;
        }

        /** Name of the database user to be used when connecting to MongoDB. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to be used when connecting to MongoDB. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /** Name of the database to watch for changes. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /** Name of the collection in the database to watch for changes. */
        public Builder<T> collection(String collection) {
            this.collection = collection;
            return this;
        }

        /**
         * An array of objects describing the pipeline operations to run. eg. [{"$match":
         * {"operationType": "insert"}}, {"$addFields": {"Kafka": "Rules!"}}]
         */
        public Builder<T> pipeline(String pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        /**
         * batch.size
         *
         * <p>The cursor batch size. Default: 0
         */
        public Builder<T> batchSize(int batchSize) {
            checkArgument(batchSize >= 0);
            this.batchSize = batchSize;
            return this;
        }

        /**
         * poll.await.time.ms
         *
         * <p>The amount of time to wait before checking for new results on the change stream.
         * Default: 3000
         */
        public Builder<T> pollAwaitTimeMillis(int pollAwaitTimeMillis) {
            checkArgument(pollAwaitTimeMillis > 0);
            this.pollAwaitTimeMillis = pollAwaitTimeMillis;
            return this;
        }

        /**
         * poll.max.batch.size
         *
         * <p>Maximum number of change stream documents to include in a single batch when polling
         * for new data. This setting can be used to limit the amount of data buffered internally in
         * the connector. Default: 1000
         */
        public Builder<T> pollMaxBatchSize(int pollMaxBatchSize) {
            checkArgument(pollMaxBatchSize > 0);
            this.pollMaxBatchSize = pollMaxBatchSize;
            return this;
        }

        /**
         * copy.existing
         *
         * <p>Copy existing data from source collections and convert them to Change Stream events on
         * their respective topics. Any changes to the data that occur during the copy process are
         * applied once the copy is completed.
         */
        public Builder<T> copyExisting(boolean copyExisting) {
            this.copyExisting = copyExisting;
            return this;
        }

        /**
         * copy.existing.max.threads
         *
         * <p>The number of threads to use when performing the data copy. Defaults to the number of
         * processors. Default: defaults to the number of processors
         */
        public Builder<T> copyExistingMaxThreads(int copyExistingMaxThreads) {
            checkArgument(copyExistingMaxThreads > 0);
            this.copyExistingMaxThreads = copyExistingMaxThreads;
            return this;
        }

        /**
         * copy.existing.queue.size
         *
         * <p>The max size of the queue to use when copying data. Default: 16000
         */
        public Builder<T> copyExistingQueueSize(int copyExistingQueueSize) {
            checkArgument(copyExistingQueueSize > 0);
            this.copyExistingQueueSize = copyExistingQueueSize;
            return this;
        }

        /**
         * copy.existing.pipeline eg. [ { "$match": { "closed": "false" } } ]
         *
         * <p>An array of JSON objects describing the pipeline operations to run when copying
         * existing data. This can improve the use of indexes by the copying manager and make
         * copying more efficient.
         */
        public Builder<T> copyExistingPipeline(String copyExistingPipeline) {
            this.copyExistingPipeline = copyExistingPipeline;
            return this;
        }

        /**
         * errors.log.enable
         *
         * <p>Whether details of failed operations should be written to the log file. When set to
         * true, both errors that are tolerated (determined by the errors.tolerance setting) and not
         * tolerated are written. When set to false, errors that are tolerated are omitted.
         */
        public Builder<T> errorsLogEnable(boolean errorsLogEnable) {
            this.errorsLogEnable = errorsLogEnable;
            return this;
        }

        /**
         * errors.tolerance
         *
         * <p>Whether to continue processing messages if an error is encountered. When set to none,
         * the connector reports an error and blocks further processing of the rest of the records
         * when it encounters an error. When set to all, the connector silently ignores any bad
         * messages.
         *
         * <p>Default: "none" Accepted Values: "none" or "all"
         */
        public Builder<T> errorsTolerance(String errorsTolerance) {
            this.errorsTolerance = errorsTolerance;
            return this;
        }

        /**
         * heartbeat.interval.ms
         *
         * <p>The length of time in milliseconds between sending heartbeat messages. Heartbeat
         * messages contain the post batch resume token and are sent when no source records have
         * been published in the specified interval. This improves the resumability of the connector
         * for low volume namespaces. Use 0 to disable.
         */
        public Builder<T> heartbeatIntervalMillis(int heartbeatIntervalMillis) {
            checkArgument(heartbeatIntervalMillis >= 0);
            this.heartbeatIntervalMillis = heartbeatIntervalMillis;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link
         * org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /** Build connection uri. */
        @VisibleForTesting
        public ConnectionString buildConnectionUri() {
            StringBuilder sb = new StringBuilder(MONGODB_SCHEME).append("://");

            if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
                sb.append(encodeValue(username))
                        .append(":")
                        .append(encodeValue(password))
                        .append("@");
            }

            sb.append(checkNotNull(hosts));

            if (StringUtils.isNotEmpty(connectionOptions)) {
                sb.append("/?").append(connectionOptions);
            }

            return new ConnectionString(sb.toString());
        }

        /**
         * The properties of mongodb kafka connector.
         * https://docs.mongodb.com/kafka-connector/current/kafka-source
         */
        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();

            props.setProperty(
                    "connector.class", MongoDBConnectorSourceConnector.class.getCanonicalName());
            props.setProperty("name", "mongodb_binlog_source");

            props.setProperty(
                    MongoSourceConfig.CONNECTION_URI_CONFIG, String.valueOf(buildConnectionUri()));

            props.setProperty(MongoSourceConfig.DATABASE_CONFIG, checkNotNull(database));
            props.setProperty(MongoSourceConfig.COLLECTION_CONFIG, checkNotNull(collection));

            props.setProperty(MongoSourceConfig.FULL_DOCUMENT_CONFIG, FULL_DOCUMENT_UPDATE_LOOKUP);
            props.setProperty(
                    MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                    String.valueOf(Boolean.FALSE));

            props.setProperty(MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG, OUTPUT_FORMAT_SCHEMA);
            props.setProperty(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OUTPUT_FORMAT_SCHEMA);
            props.setProperty(
                    MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG,
                    String.valueOf(Boolean.FALSE));
            props.setProperty(
                    MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG, OUTPUT_SCHEMA_VALUE_DEFAULT);

            if (pipeline != null) {
                props.setProperty(MongoSourceConfig.PIPELINE_CONFIG, pipeline);
            }

            if (batchSize != null) {
                props.setProperty(MongoSourceConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
            }

            if (pollAwaitTimeMillis != null) {
                props.setProperty(
                        MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG,
                        String.valueOf(pollAwaitTimeMillis));
            }

            if (pollMaxBatchSize != null) {
                props.setProperty(
                        MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG,
                        String.valueOf(pollMaxBatchSize));
            }

            if (errorsLogEnable != null) {
                props.setProperty(
                        MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG,
                        String.valueOf(errorsLogEnable));
            }

            if (errorsTolerance != null) {
                props.setProperty(MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, errorsTolerance);
            }

            if (copyExisting != null) {
                props.setProperty(
                        MongoSourceConfig.COPY_EXISTING_CONFIG, String.valueOf(copyExisting));
            }

            if (copyExistingMaxThreads != null) {
                props.setProperty(
                        MongoSourceConfig.COPY_EXISTING_MAX_THREADS_CONFIG,
                        String.valueOf(copyExistingMaxThreads));
            }

            if (copyExistingQueueSize != null) {
                props.setProperty(
                        MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_CONFIG,
                        String.valueOf(copyExistingQueueSize));
            }

            if (copyExistingPipeline != null) {
                props.setProperty(
                        MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG, copyExistingPipeline);
            }

            if (heartbeatIntervalMillis != null) {
                props.setProperty(
                        MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                        String.valueOf(heartbeatIntervalMillis));
            }

            props.setProperty(
                    MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, HEARTBEAT_TOPIC_NAME_DEFAULT);

            // Let DebeziumChangeFetcher recognize heartbeat record
            props.setProperty(
                    Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(), HEARTBEAT_TOPIC_NAME_DEFAULT);

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, Validator.getDefaultValidator());
        }
    }
}
