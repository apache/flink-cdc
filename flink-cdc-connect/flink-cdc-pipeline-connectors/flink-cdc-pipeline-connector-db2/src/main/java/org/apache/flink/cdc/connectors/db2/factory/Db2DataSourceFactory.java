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

package org.apache.flink.cdc.connectors.db2.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.source.Db2DataSource;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.table.Db2ReadableMetaData;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.table.api.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link Db2DataSource}. */
@Internal
public class Db2DataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(Db2DataSourceFactory.class);

    public static final String IDENTIFIER = "db2";

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hostname = checkNotNull(config.get(HOSTNAME));
        int port = config.get(PORT);
        String username = checkNotNull(config.get(USERNAME));
        String password = checkNotNull(config.get(PASSWORD));
        String database = checkNotNull(config.get(DATABASE));
        String chunkKeyColumn = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        String tables = checkNotNull(config.get(TABLES));
        ZoneId serverTimeZone = getServerTimeZone(config);
        String tablesExclude = config.get(TABLES_EXCLUDE);
        StartupOptions startupOptions = getStartupOptions(config);

        boolean includeSchemaChanges = config.get(SCHEMA_CHANGE_ENABLED);

        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean isAssignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        Map<String, String> configMap = config.toMap();

        Db2SourceConfigFactory configFactory = new Db2SourceConfigFactory();
        configFactory
                .hostname(hostname)
                .port(port)
                .databaseList(database)
                .tableList(".*\\..*")
                .username(username)
                .password(password)
                .serverTimeZone(serverTimeZone.getId())
                .debeziumProperties(getDebeziumProperties(configMap))
                .splitSize(splitSize)
                .splitMetaGroupSize(splitMetaGroupSize)
                .distributionFactorUpper(distributionFactorUpper)
                .distributionFactorLower(distributionFactorLower)
                .fetchSize(fetchSize)
                .connectTimeout(connectTimeout)
                .connectMaxRetries(connectMaxRetries)
                .connectionPoolSize(connectionPoolSize)
                .includeSchemaChanges(includeSchemaChanges)
                .startupOptions(startupOptions)
                .chunkKeyColumn(chunkKeyColumn)
                .closeIdleReaders(closeIdleReaders)
                .skipSnapshotBackfill(skipSnapshotBackfill)
                .assignUnboundedChunkFirst(isAssignUnboundedChunkFirst);

        Db2SourceConfig sourceConfig = configFactory.create(0);
        List<TableId> tableIds = Db2SchemaUtils.listTables(sourceConfig, null);

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        List<String> capturedTables = getTableList(tableIds, selectors);
        if (capturedTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        if (tablesExclude != null) {
            Selectors selectExclude =
                    new Selectors.SelectorsBuilder().includeTables(tablesExclude).build();
            List<String> excludeTables = getTableList(tableIds, selectExclude);
            if (!excludeTables.isEmpty()) {
                capturedTables.removeAll(excludeTables);
            }
            if (capturedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot find any table with by the option 'tables.exclude' = "
                                + tablesExclude);
            }
        }
        configFactory.tableList(capturedTables.toArray(new String[0]));

        String metadataList = config.get(METADATA_LIST);
        List<Db2ReadableMetaData> readableMetadataList = listReadableMetadata(metadataList);

        return new Db2DataSource(configFactory, readableMetadataList);
    }

    private List<Db2ReadableMetaData> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return new ArrayList<>();
        }
        Set<String> readableMetadataSet =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<Db2ReadableMetaData> foundMetadata = new ArrayList<>();
        for (Db2ReadableMetaData metadata : Db2ReadableMetaData.values()) {
            if (readableMetadataSet.contains(metadata.getKey())) {
                foundMetadata.add(metadata);
                readableMetadataSet.remove(metadata.getKey());
            }
        }
        if (readableMetadataSet.isEmpty()) {
            return foundMetadata;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] cannot be found in Db2 metadata.",
                        String.join(", ", readableMetadataSet)));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(TABLES_EXCLUDE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCHEMA_CHANGE_ENABLED);
        options.add(SCAN_STARTUP_MODE);
        options.add(SERVER_TIME_ZONE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(METADATA_LIST);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static List<String> getTableList(List<TableId> tableIdList, Selectors selectors) {
        List<TableId> resolvedTableIdList =
                tableIdList == null ? Collections.emptyList() : tableIdList;
        return resolvedTableIdList.stream()
                .filter(selectors::isMatch)
                // Db2 tableList format: schemaName.tableName (without database prefix)
                // See Db2SourceBuilder: "Each identifier is of the form <schemaName>.<tableName>"
                .map(tableId -> tableId.getSchemaName() + "." + tableId.getTableName())
                .collect(Collectors.toList());
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

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(Configuration config) {
        final String serverTimeZone = config.get(SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOG.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    SERVER_TIME_ZONE.key());
            return ZoneId.systemDefault();
        }
    }
}
