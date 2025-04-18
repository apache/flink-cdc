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

package com.apache.flink.cdc.connectors.oracle.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.table.OracleReadableMetaData;
import org.apache.flink.table.api.ValidationException;

import com.apache.flink.cdc.connectors.oracle.source.OracleDataSource;
import com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions;
import com.apache.flink.cdc.connectors.oracle.utils.OracleSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link OracleDataSource}. */
@Internal
public class OracleDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OracleDataSourceFactory.class);

    public static final String IDENTIFIER = "oracle";
    private String[] capturedTables;

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
        OracleSourceConfigFactory configFactory =
                (OracleSourceConfigFactory)
                        new OracleSourceConfigFactory()
                                .hostname(
                                        config.getOptional(OracleDataSourceOptions.HOSTNAME).get())
                                .port(config.getOptional(OracleDataSourceOptions.PORT).get())
                                .databaseList(
                                        config.getOptional(OracleDataSourceOptions.DATABASE)
                                                .get()) // monitor oracledatabase
                                .tableList(
                                        config.getOptional(OracleDataSourceOptions.TABLES)
                                                .get()) // monitor productstable
                                .username(
                                        config.getOptional(OracleDataSourceOptions.USERNAME).get())
                                .password(
                                        config.getOptional(OracleDataSourceOptions.PASSWORD).get())
                                .includeSchemaChanges(true);
        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        String[] capturedTables = getTableList(configFactory.create(0), selectors);
        if (capturedTables.length == 0) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        configFactory.tableList(capturedTables);
        configFactory.databaseList(config.getOptional(OracleDataSourceOptions.DATABASE).get());
        configFactory.schemaList(
                new String[] {config.getOptional(OracleDataSourceOptions.SCHEMALIST).get()});
        String metadataList = config.get(METADATA_LIST);
        List<OracleReadableMetaData> readableMetadataList = listReadableMetadata(metadataList);
        return new OracleDataSource(configFactory, config, capturedTables, readableMetadataList);
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
                        "[%s] cannot be found in mysql metadata.",
                        String.join(", ", readableMetadataList)));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.HOSTNAME);
        options.add(OracleDataSourceOptions.PORT);
        options.add(OracleDataSourceOptions.USERNAME);
        options.add(OracleDataSourceOptions.PASSWORD);
        options.add(OracleDataSourceOptions.SCHEMALIST);
        options.add(OracleDataSourceOptions.DATABASE);
        options.add(OracleDataSourceOptions.TABLES);
        options.add(METADATA_LIST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.SERVER_TIME_ZONE);
        options.add(OracleDataSourceOptions.SERVER_ID);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_MODE);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
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
        options.add(OracleDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(OracleDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(OracleDataSourceOptions.SNAPSHOT_LOCKING_MODE);
        options.add(OracleDataSourceOptions.HISTORY_CAPTURED_TABLES_DDL_ENABLE);
        options.add(OracleDataSourceOptions.LOG_MINING_CONTINUOUS_MINE);
        options.add(OracleDataSourceOptions.LOG_MINING_STRATEGY);
        options.add(OracleDataSourceOptions.DATABASE_CONNECTION_ADAPTER);
        options.add(OracleDataSourceOptions.DATABASE_TABLE_CASE_INSENSITIVE);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static String[] getTableList(OracleSourceConfig sourceConfig, Selectors selectors) {
        return OracleSchemaUtils.listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .toArray(String[]::new);
    }

    // todo: support oracle startup mode
    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(OracleDataSourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        config.get(OracleDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                OracleDataSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
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

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(Configuration config) {
        final String serverTimeZone = config.get(OracleDataSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOG.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    OracleDataSourceOptions.SERVER_TIME_ZONE.key());
            return ZoneId.systemDefault();
        }
    }
}
