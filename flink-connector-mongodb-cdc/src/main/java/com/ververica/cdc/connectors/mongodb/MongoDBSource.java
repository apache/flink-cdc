/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.ErrorTolerance;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBConnectorSourceConnector;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import io.debezium.heartbeat.Heartbeat;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBConnectorSourceTask.COLLECTION_INCLUDE_LIST;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBConnectorSourceTask.DATABASE_INCLUDE_LIST;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_TOPIC_NAME;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.OUTPUT_SCHEMA;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COPY_EXISTING;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.buildConnectionString;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume change stream
 * events.
 */
@PublicEvolving
public class MongoDBSource {

    public static final String FULL_DOCUMENT_UPDATE_LOOKUP = FullDocument.UPDATE_LOOKUP.getValue();

    public static final String OUTPUT_FORMAT_SCHEMA =
            OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT);

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link MongoDBSource}. */
    public static class Builder<T> {

        private String hosts;
        private String username;
        private String password;
        private List<String> databaseList;
        private List<String> collectionList;
        private String connectionOptions;
        private Integer batchSize = BATCH_SIZE.defaultValue();
        private Integer pollAwaitTimeMillis = POLL_AWAIT_TIME_MILLIS.defaultValue();
        private Integer pollMaxBatchSize = POLL_MAX_BATCH_SIZE.defaultValue();
        private Boolean updateLookup = true;
        private Boolean copyExisting = COPY_EXISTING.defaultValue();
        private Integer copyExistingMaxThreads;
        private Integer copyExistingQueueSize;
        private String copyExistingPipeline;
        private Integer heartbeatIntervalMillis = HEARTBEAT_INTERVAL_MILLIS.defaultValue();
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

        /** Regular expressions list that match database names to be monitored. */
        public Builder<T> databaseList(String... databaseList) {
            this.databaseList = Arrays.asList(databaseList);
            return this;
        }

        /**
         * Regular expressions that match fully-qualified collection identifiers for collections to
         * be monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
         */
        public Builder<T> collectionList(String... collectionList) {
            this.collectionList = Arrays.asList(collectionList);
            return this;
        }

        /**
         * batch.size
         *
         * <p>The cursor batch size. Default: 1024
         *
         * <p>The change stream cursor batch size. Specifies the maximum number of change events to
         * return in each batch of the response from the MongoDB cluster. The default is 0 meaning
         * it uses the server's default value. Default: 0
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
         * Default: 1000
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
         * the connector. Default: 1024
         */
        public Builder<T> pollMaxBatchSize(int pollMaxBatchSize) {
            checkArgument(pollMaxBatchSize > 0);
            this.pollMaxBatchSize = pollMaxBatchSize;
            return this;
        }

        /**
         * change.stream.full.document
         *
         * <p>Determines what to return for update operations when using a Change Stream. When set
         * to true, the change stream for partial updates will include both a delta describing the
         * changes to the document and a copy of the entire document that was changed from some time
         * after the change occurred. Default: true
         */
        public Builder<T> updateLookup(boolean updateLookup) {
            this.updateLookup = updateLookup;
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
         * <p>The max size of the queue to use when copying data. Default: 10240
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

        /**
         * The properties of mongodb kafka connector.
         * https://docs.mongodb.com/kafka-connector/current/kafka-source
         */
        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();

            props.setProperty(
                    "connector.class", MongoDBConnectorSourceConnector.class.getCanonicalName());
            props.setProperty("name", "mongodb_cdc_source");

            props.setProperty(
                    MongoSourceConfig.CONNECTION_URI_CONFIG,
                    String.valueOf(
                            buildConnectionString(username, password, hosts, connectionOptions)));

            if (databaseList != null) {
                props.setProperty(DATABASE_INCLUDE_LIST, String.join(",", databaseList));
            }

            if (collectionList != null) {
                props.setProperty(COLLECTION_INCLUDE_LIST, String.join(",", collectionList));
            }

            if (updateLookup) {
                props.setProperty(
                        MongoSourceConfig.FULL_DOCUMENT_CONFIG, FULL_DOCUMENT_UPDATE_LOOKUP);
            }

            props.setProperty(
                    MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                    String.valueOf(Boolean.FALSE));

            props.setProperty(MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG, OUTPUT_FORMAT_SCHEMA);
            props.setProperty(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OUTPUT_FORMAT_SCHEMA);
            props.setProperty(
                    MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG,
                    String.valueOf(Boolean.FALSE));
            props.setProperty(MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG, OUTPUT_SCHEMA);

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

            props.setProperty(MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, HEARTBEAT_TOPIC_NAME);

            // Let DebeziumChangeFetcher recognize heartbeat record
            props.setProperty(Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(), HEARTBEAT_TOPIC_NAME);

            props.setProperty(
                    MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG, String.valueOf(Boolean.TRUE));
            props.setProperty(
                    MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.NONE.value());

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, Validator.getDefaultValidator());
        }
    }
}
