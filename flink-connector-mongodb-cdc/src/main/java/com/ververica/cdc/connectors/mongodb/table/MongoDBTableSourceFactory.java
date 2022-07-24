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

package com.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.mongodb.MongoDBSource.BATCH_SIZE_DEFAULT;
import static com.ververica.cdc.connectors.mongodb.MongoDBSource.ERROR_TOLERANCE_NONE;
import static com.ververica.cdc.connectors.mongodb.MongoDBSource.POLL_AWAIT_TIME_MILLIS_DEFAULT;
import static com.ververica.cdc.connectors.mongodb.MongoDBSource.POLL_MAX_BATCH_SIZE_DEFAULT;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Factory for creating configured instance of {@link MongoDBTableSource}. */
public class MongoDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mongodb-cdc";

    private static final String DOCUMENT_ID_FIELD = "_id";

    private static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of hostname and port pairs of the MongoDB servers. "
                                    + "eg. localhost:27017,localhost:27018");

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database user to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    private static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to watch for changes."
                                    + "The database also supports regular expression "
                                    + "to monitor multiple databases matches the regular expression."
                                    + "e.g. db[0-9] .");

    private static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the collection in the database to watch for changes."
                                    + "The collection also supports regular expression "
                                    + "to monitor multiple collections matches fully-qualified collection identifiers."
                                    + "e.g. db0\\.coll[0-9] .");

    private static final ConfigOption<String> CONNECTION_OPTIONS =
            ConfigOptions.key("connection.options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ampersand-separated MongoDB connection options. "
                                    + "eg. replicaSet=test&connectTimeoutMS=300000");

    private static final ConfigOption<String> ERRORS_TOLERANCE =
            ConfigOptions.key("errors.tolerance")
                    .stringType()
                    .defaultValue(ERROR_TOLERANCE_NONE)
                    .withDescription(
                            "Whether to continue processing messages if an error is encountered. "
                                    + "When set to none, the connector reports an error and blocks further processing "
                                    + "of the rest of the records when it encounters an error. "
                                    + "When set to all, the connector silently ignores any bad messages."
                                    + "Accepted Values: 'none' or 'all'. Default 'none'.");

    private static final ConfigOption<Boolean> ERRORS_LOG_ENABLE =
            ConfigOptions.key("errors.log.enable")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription(
                            "Whether details of failed operations should be written to the log file. "
                                    + "When set to true, both errors that are tolerated (determined by the errors.tolerance setting) "
                                    + "and not tolerated are written. When set to false, errors that are tolerated are omitted.");

    private static final ConfigOption<Boolean> COPY_EXISTING =
            ConfigOptions.key("copy.existing")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription(
                            "Copy existing data from source collections and convert them "
                                    + "to Change Stream events on their respective topics. Any changes to the data "
                                    + "that occur during the copy process are applied once the copy is completed.");

    private static final ConfigOption<String> COPY_EXISTING_PIPELINE =
            ConfigOptions.key("copy.existing.pipeline")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An array of JSON objects describing the pipeline operations "
                                    + "to run when copying existing data. "
                                    + "This can improve the use of indexes by the copying manager and make copying more efficient.");

    private static final ConfigOption<Integer> COPY_EXISTING_MAX_THREADS =
            ConfigOptions.key("copy.existing.max.threads")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The number of threads to use when performing the data copy."
                                    + " Defaults to the number of processors.");

    private static final ConfigOption<Integer> COPY_EXISTING_QUEUE_SIZE =
            ConfigOptions.key("copy.existing.queue.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The max size of the queue to use when copying data. Defaults to 16000.");

    private static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(BATCH_SIZE_DEFAULT)
                    .withDescription(
                            "Change stream cursor batch size. "
                                    + "Specifies the maximum number of change events to return in each batch "
                                    + "of the response from the MongoDB cluster."
                                    + "Defaults to 0 meaning it uses the server's default value.");

    private static final ConfigOption<Integer> POLL_MAX_BATCH_SIZE =
            ConfigOptions.key("poll.max.batch.size")
                    .intType()
                    .defaultValue(POLL_MAX_BATCH_SIZE_DEFAULT)
                    .withDescription(
                            "Maximum number of change stream documents "
                                    + "to include in a single batch when polling for new data. "
                                    + "This setting can be used to limit the amount of data buffered internally in the connector. "
                                    + "Defaults to 1000.");

    private static final ConfigOption<Integer> POLL_AWAIT_TIME_MILLIS =
            ConfigOptions.key("poll.await.time.ms")
                    .intType()
                    .defaultValue(POLL_AWAIT_TIME_MILLIS_DEFAULT)
                    .withDescription(
                            "The amount of time to wait before checking for new results on the change stream."
                                    + "Defaults: 1500.");

    private static final ConfigOption<Integer> HEARTBEAT_INTERVAL_MILLIS =
            ConfigOptions.key("heartbeat.interval.ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The length of time in milliseconds between sending heartbeat messages."
                                    + "Heartbeat messages contain the post batch resume token and are sent when no source records "
                                    + "have been published in the specified interval. This improves the resumability of the connector "
                                    + "for low volume namespaces. Use 0 to disable. Defaults to 0.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String hosts = config.get(HOSTS);
        String connectionOptions = config.getOptional(CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);

        String database = config.getOptional(DATABASE).orElse(null);
        String collection = config.getOptional(COLLECTION).orElse(null);

        String errorsTolerance = config.get(ERRORS_TOLERANCE);
        Boolean errorsLogEnable = config.get(ERRORS_LOG_ENABLE);

        Integer batchSize = config.get(BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis =
                config.getOptional(HEARTBEAT_INTERVAL_MILLIS).orElse(null);

        Boolean copyExisting = config.get(COPY_EXISTING);
        String copyExistingPipeline = config.getOptional(COPY_EXISTING_PIPELINE).orElse(null);
        Integer copyExistingMaxThreads = config.getOptional(COPY_EXISTING_MAX_THREADS).orElse(null);
        Integer copyExistingQueueSize = config.getOptional(COPY_EXISTING_QUEUE_SIZE).orElse(null);

        String zoneId = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        ZoneId localTimeZone =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneId);

        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        checkArgument(physicalSchema.getPrimaryKey().isPresent(), "Primary key must be present");
        checkPrimaryKey(physicalSchema.getPrimaryKey().get(), "Primary key must be _id field");

        return new MongoDBTableSource(
                physicalSchema,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                errorsTolerance,
                errorsLogEnable,
                copyExisting,
                copyExistingPipeline,
                copyExistingMaxThreads,
                copyExistingQueueSize,
                batchSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone);
    }

    private void checkPrimaryKey(UniqueConstraint pk, String message) {
        checkArgument(
                pk.getColumns().size() == 1 && pk.getColumns().contains(DOCUMENT_ID_FIELD),
                message);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECTION_OPTIONS);
        options.add(DATABASE);
        options.add(COLLECTION);
        options.add(ERRORS_TOLERANCE);
        options.add(ERRORS_LOG_ENABLE);
        options.add(COPY_EXISTING);
        options.add(COPY_EXISTING_PIPELINE);
        options.add(COPY_EXISTING_MAX_THREADS);
        options.add(COPY_EXISTING_QUEUE_SIZE);
        options.add(BATCH_SIZE);
        options.add(POLL_MAX_BATCH_SIZE);
        options.add(POLL_AWAIT_TIME_MILLIS);
        options.add(HEARTBEAT_INTERVAL_MILLIS);
        return options;
    }
}
