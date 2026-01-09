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

package org.apache.flink.cdc.connectors.oracle.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.source.OracleDataSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.table.OracleReadableMetaData;
import org.apache.flink.cdc.connectors.oracle.utils.OracleSchemaUtils;
import org.apache.flink.table.api.ValidationException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link OracleDataSource}. */
@Internal
public class OracleDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "oracle";
    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String DEBEZIUM_PROPERTIES_PREFIX = "debezium.";

    @Override
    public DataSource createDataSource(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        int fetchSize = config.get(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper =
                config.get(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        int connectMaxRetries = config.get(OracleDataSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(OracleDataSourceOptions.CONNECTION_POOL_SIZE);
        String tables = config.get(OracleDataSourceOptions.TABLES);
        validateIntegerOption(
                OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);
        String metadataList = config.get(METADATA_LIST);
        List<OracleReadableMetaData> readableMetadataList = listReadableMetadata(metadataList);

        String url = config.get(OracleDataSourceOptions.JDBC_URL);
        int port = checkNotNull(config.get(OracleDataSourceOptions.PORT));
        String hostname = checkNotNull(config.get(OracleDataSourceOptions.HOSTNAME));
        String database = checkNotNull(config.get(OracleDataSourceOptions.DATABASE));
        String username = checkNotNull(config.get(OracleDataSourceOptions.USERNAME));
        String password = checkNotNull(config.get(OracleDataSourceOptions.PASSWORD));
        boolean schemaChangeEnabled = config.get(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED);
        String serverTimeZone = config.get(OracleDataSourceOptions.SERVER_TIME_ZONE);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(
                "database.connection.adapter",
                config.get(OracleDataSourceOptions.DATABASE_CONNECTION_ADAPTER));
        dbzProperties.setProperty(
                "log.mining.strategy", config.get(OracleDataSourceOptions.LOG_MINING_STRATEGY));

        Map<String, String> map =
                OracleDataSourceOptions.getPropertiesByPrefix(config, DEBEZIUM_PROPERTIES_PREFIX);
        map.keySet().forEach(e -> dbzProperties.put(e, map.get(e)));
        StartupOptions startupOptions = getStartupOptions(config);

        Duration connectTimeout = config.get(OracleDataSourceOptions.CONNECT_TIMEOUT);

        boolean closeIdleReaders =
                config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill =
                config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.url(url);
        configFactory.hostname(hostname);
        configFactory.port(port);
        configFactory.databaseList(database);
        configFactory.username(username);
        configFactory.password(password);
        configFactory.startupOptions(startupOptions);
        configFactory.debeziumProperties(dbzProperties);
        configFactory.splitSize(splitSize);
        configFactory.splitMetaGroupSize(splitMetaGroupSize);
        configFactory.fetchSize(fetchSize);
        configFactory.connectTimeout(connectTimeout);
        configFactory.connectionPoolSize(connectionPoolSize);
        configFactory.connectMaxRetries(connectMaxRetries);
        configFactory.distributionFactorUpper(distributionFactorUpper);
        configFactory.distributionFactorLower(distributionFactorLower);
        configFactory.closeIdleReaders(closeIdleReaders);
        configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        configFactory.includeSchemaChanges(schemaChangeEnabled);
        configFactory.serverTimeZone(serverTimeZone);
        configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        String[] capturedTables = getTableList(configFactory.create(0), selectors);
        if (capturedTables.length == 0) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        configFactory.tableList(capturedTables);
        return new OracleDataSource(configFactory, config, readableMetadataList);
    }

    public static List<OracleReadableMetaData> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return new ArrayList<>();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<OracleReadableMetaData> foundMetadata = new ArrayList<>();
        for (OracleReadableMetaData metadata : OracleReadableMetaData.values()) {
            if (readableMetadataList.contains(metadata.getKey())) {
                foundMetadata.add(metadata);
                readableMetadataList.remove(metadata.getKey());
            }
        }
        if (readableMetadataList.isEmpty()) {
            return foundMetadata;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] cannot be found in oracle metadata.",
                        String.join(", ", readableMetadataList)));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.HOSTNAME);
        options.add(OracleDataSourceOptions.PORT);
        options.add(OracleDataSourceOptions.USERNAME);
        options.add(OracleDataSourceOptions.PASSWORD);
        options.add(OracleDataSourceOptions.DATABASE);
        options.add(OracleDataSourceOptions.TABLES);
        options.add(OracleDataSourceOptions.METADATA_LIST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.JDBC_URL);
        options.add(OracleDataSourceOptions.SERVER_TIME_ZONE);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(OracleDataSourceOptions.CONNECT_TIMEOUT);
        options.add(OracleDataSourceOptions.CONNECTION_POOL_SIZE);
        options.add(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(OracleDataSourceOptions.CONNECT_MAX_RETRIES);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(OracleDataSourceOptions.HEARTBEAT_INTERVAL);
        options.add(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(OracleDataSourceOptions.LOG_MINING_CONTINUOUS_MINE);
        options.add(OracleDataSourceOptions.LOG_MINING_STRATEGY);
        options.add(OracleDataSourceOptions.DATABASE_CONNECTION_ADAPTER);
        options.add(OracleDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_MODE);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static String[] getTableList(OracleSourceConfig sourceConfig, Selectors selectors) {
        return OracleSchemaUtils.listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .toArray(String[]::new);
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
                        OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .key(),
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
                        OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
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

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }
}
