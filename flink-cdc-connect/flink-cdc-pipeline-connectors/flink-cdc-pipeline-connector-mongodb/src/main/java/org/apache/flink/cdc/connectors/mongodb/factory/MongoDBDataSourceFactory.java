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

package org.apache.flink.cdc.connectors.mongodb.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSource;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSchemaUtils;
import org.apache.flink.table.api.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.CONNECTION_OPTIONS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.HOSTS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.POLL_MAX_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_CHANGESTREAM_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_NO_CURSOR_TIMEOUT;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link MongoDBDataSourceFactory}. */
@Internal
public class MongoDBDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBDataSourceFactory.class);

    public static final String IDENTIFIER = "mongodb";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hosts = config.get(HOSTS);
        String connectionOptions = config.get(CONNECTION_OPTIONS);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String tables = config.get(TABLES);

        StartupOptions startupOptions = getStartupOptions(config);

        int batchSize = config.get(BATCH_SIZE);

        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int samplesPerChunk = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);

        int pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);
        int pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Boolean scanFullChangelog = config.get(FULL_DOCUMENT_PRE_POST_IMAGE);
        Boolean disableCursorTimeout = config.get(SCAN_NO_CURSOR_TIMEOUT);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);

        int heartbeatInterval = config.get(HEARTBEAT_INTERVAL_MILLIS);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        Boolean scanChangeStreamNewlyAddedTableEnabled =
                config.get(SCAN_CHANGESTREAM_NEWLY_ADDED_TABLE_ENABLED);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB, splitSize, 1);
        validateIntegerOption(BATCH_SIZE, batchSize, 1);

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(hosts)
                        .connectionOptions(connectionOptions)
                        .username(username)
                        .password(password)
                        .databaseList(".*")
                        .collectionList(".*")
                        .startupOptions(startupOptions)
                        .batchSize(batchSize)
                        .pollAwaitTimeMillis(pollAwaitTimeMillis)
                        .pollMaxBatchSize(pollMaxBatchSize)
                        .heartbeatIntervalMillis(heartbeatInterval)
                        .splitSizeMB(splitSize)
                        .splitMetaGroupSize(samplesPerChunk)
                        .samplesPerChunk(samplesPerChunk)
                        .closeIdleReaders(closeIdleReaders)
                        .scanFullChangelog(scanFullChangelog)
                        .disableCursorTimeout(disableCursorTimeout)
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);

        List<TableId> tableIds = MongoDBSchemaUtils.listTables(configFactory.create(0), null);

        if (scanChangeStreamNewlyAddedTableEnabled && scanNewlyAddedTableEnabled) {
            throw new IllegalArgumentException(
                    "If both scan.binlog.newly-added-table.enabled and scan.newly-added-table.enabled are true, data maybe duplicate after restore");
        }

        if (scanChangeStreamNewlyAddedTableEnabled) {
            String newTables = validateTableAndReturnDebeziumStyle(tables);
            configFactory.collectionList(newTables);
        } else {
            Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
            List<String> capturedTables = getTableList(tableIds, selectors);
            if (capturedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot find any collection by the option 'tables' = " + tables);
            }
            configFactory.collectionList(capturedTables.toArray(new String[0]));
        }
        return new MongoDBDataSource(configFactory);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(TABLES);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static List<String> getTableList(
            @Nullable List<TableId> tableIdList, Selectors selectors) {
        return tableIdList.stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();

            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(config.get(SCAN_STARTUP_TIMESTAMP_MILLIS));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    private String validateTableAndReturnDebeziumStyle(String tables) {
        // MySQL table names are not allowed to have `,` character.
        if (tables.contains(",")) {
            throw new IllegalArgumentException(
                    "the `,` in "
                            + tables
                            + " is not supported when "
                            + SCAN_CHANGESTREAM_NEWLY_ADDED_TABLE_ENABLED
                            + " was enabled.");
        }

        return tables.replace("\\.", ".");
    }
}
