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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.config.ServerIdRange;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.mysql.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for creating configured instance of {@link MySqlTableSource}. */
public class MySqlTableSourceFactory implements DynamicTableSourceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlTableSourceFactory.class);

    private static final String IDENTIFIER = "mysql-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX, JdbcUrlUtils.PROPERTIES_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(MySqlSourceOptions.HOSTNAME);
        String username = config.get(MySqlSourceOptions.USERNAME);
        String password = config.get(MySqlSourceOptions.PASSWORD);
        String databaseName = config.get(MySqlSourceOptions.DATABASE_NAME);
        validateRegex(MySqlSourceOptions.DATABASE_NAME.key(), databaseName);
        String tableName = config.get(MySqlSourceOptions.TABLE_NAME);
        validateRegex(MySqlSourceOptions.TABLE_NAME.key(), tableName);
        int port = config.get(MySqlSourceOptions.PORT);
        int splitSize = config.get(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(MySqlSourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        ZoneId serverTimeZone = getServerTimeZone(config);

        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        String serverId = validateAndGetServerId(config);
        StartupOptions startupOptions = getStartupOptions(config);
        Duration connectTimeout = config.get(MySqlSourceOptions.CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(MySqlSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(MySqlSourceOptions.CONNECTION_POOL_SIZE);
        double distributionFactorUpper =
                config.get(MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        boolean scanNewlyAddedTableEnabled =
                config.get(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        Duration heartbeatInterval = config.get(MySqlSourceOptions.HEARTBEAT_INTERVAL);
        String chunkKeyColumn =
                config.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);

        boolean enableParallelRead =
                config.get(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        boolean closeIdleReaders =
                config.get(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackFill =
                config.get(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean parseOnLineSchemaChanges =
                config.get(MySqlSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES);
        boolean useLegacyJsonFormat = config.get(MySqlSourceOptions.USE_LEGACY_JSON_FORMAT);
        boolean assignUnboundedChunkFirst =
                config.get(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST);

        boolean appendOnly =
                config.get(MySqlSourceOptions.SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED);

        if (enableParallelRead) {
            validatePrimaryKeyIfEnableParallel(physicalSchema, chunkKeyColumn);
            validateIntegerOption(
                    MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(MySqlSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
            validateIntegerOption(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(MySqlSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
            validateIntegerOption(MySqlSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
            validateDistributionFactorUpper(distributionFactorUpper);
            validateDistributionFactorLower(distributionFactorLower);
            validateDurationOption(
                    MySqlSourceOptions.CONNECT_TIMEOUT, connectTimeout, Duration.ofMillis(250));
        }

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        return new MySqlTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                tableName,
                username,
                password,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                serverId,
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                startupOptions,
                scanNewlyAddedTableEnabled,
                closeIdleReaders,
                JdbcUrlUtils.getJdbcProperties(context.getCatalogTable().getOptions()),
                heartbeatInterval,
                chunkKeyColumn,
                skipSnapshotBackFill,
                parseOnLineSchemaChanges,
                useLegacyJsonFormat,
                assignUnboundedChunkFirst,
                appendOnly);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MySqlSourceOptions.HOSTNAME);
        options.add(MySqlSourceOptions.USERNAME);
        options.add(MySqlSourceOptions.PASSWORD);
        options.add(MySqlSourceOptions.DATABASE_NAME);
        options.add(MySqlSourceOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MySqlSourceOptions.PORT);
        options.add(MySqlSourceOptions.SERVER_TIME_ZONE);
        options.add(MySqlSourceOptions.SERVER_ID);
        options.add(MySqlSourceOptions.SCAN_STARTUP_MODE);
        options.add(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        options.add(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS);
        options.add(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS);
        options.add(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(MySqlSourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(MySqlSourceOptions.CONNECT_TIMEOUT);
        options.add(MySqlSourceOptions.CONNECTION_POOL_SIZE);
        options.add(MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(MySqlSourceOptions.CONNECT_MAX_RETRIES);
        options.add(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(MySqlSourceOptions.HEARTBEAT_INTERVAL);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(MySqlSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES);
        options.add(MySqlSourceOptions.USE_LEGACY_JSON_FORMAT);
        options.add(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST);
        options.add(MySqlSourceOptions.SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(MySqlSourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();

            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                validateSpecificOffset(config);
                return getSpecificOffset(config);

            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        config.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s, %s], but was: %s",
                                MySqlSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private static void validateSpecificOffset(ReadableConfig config) {
        Optional<String> gtidSet =
                config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        Optional<String> binlogFilename =
                config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Optional<Long> binlogPosition =
                config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        if (!gtidSet.isPresent() && !(binlogFilename.isPresent() && binlogPosition.isPresent())) {
            throw new ValidationException(
                    String.format(
                            "Unable to find a valid binlog offset. Either %s, or %s and %s are required.",
                            MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET.key(),
                            MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE.key(),
                            MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS.key()));
        }
    }

    private static StartupOptions getSpecificOffset(ReadableConfig config) {
        BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();

        // GTID set
        config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                .ifPresent(offsetBuilder::setGtidSet);

        // Binlog file + pos
        Optional<String> binlogFilename =
                config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Optional<Long> binlogPosition =
                config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        if (binlogFilename.isPresent() && binlogPosition.isPresent()) {
            offsetBuilder.setBinlogFilePosition(binlogFilename.get(), binlogPosition.get());
        } else {
            offsetBuilder.setBinlogFilePosition("", 0);
        }

        config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                .ifPresent(offsetBuilder::setSkipEvents);
        config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                .ifPresent(offsetBuilder::setSkipRows);
        return StartupOptions.specificOffset(offsetBuilder.build());
    }

    private void validatePrimaryKeyIfEnableParallel(
            ResolvedSchema physicalSchema, @Nullable String chunkKeyColumn) {
        if (chunkKeyColumn == null && !physicalSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    String.format(
                            "'%s' is required for table without primary key when '%s' enabled.",
                            MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                            MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key()));
        }
    }

    private String validateAndGetServerId(ReadableConfig configuration) {
        final String serverIdValue = configuration.get(MySqlSourceOptions.SERVER_ID);
        if (serverIdValue != null) {
            // validation
            try {
                ServerIdRange.from(serverIdValue);
            } catch (Exception e) {
                throw new ValidationException(
                        String.format(
                                "The value of option 'server-id' is invalid: '%s'", serverIdValue),
                        e);
            }
        }
        return serverIdValue;
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

    /** Checks the value of given duration option is valid. */
    private void validateDurationOption(
            ConfigOption<Duration> option, Duration optionValue, Duration exclusiveMin) {
        checkState(
                optionValue.toMillis() > exclusiveMin.toMillis(),
                String.format(
                        "The value of option '%s' cannot be less than %s, but actual is %s",
                        option.key(), exclusiveMin, optionValue));
    }

    /**
     * Checks the given regular expression's syntax is valid.
     *
     * @param optionName the option name of the regex
     * @param regex The regular expression to be checked
     * @throws ValidationException If the expression's syntax is invalid
     */
    private void validateRegex(String optionName, String regex) {
        try {
            Pattern.compile(regex);
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "The %s '%s' is not a valid regular expression", optionName, regex),
                    e);
        }
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                ObjectUtils.doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                ObjectUtils.doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && ObjectUtils.doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(ReadableConfig config) {
        final String serverTimeZone = config.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOGGER.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    MySqlSourceOptions.SERVER_TIME_ZONE.key());
            final String sessionTimeZone = config.get(TableConfigOptions.LOCAL_TIME_ZONE);
            final ZoneId zoneId =
                    TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(sessionTimeZone)
                            ? ZoneId.systemDefault()
                            : ZoneId.of(sessionTimeZone);

            return zoneId;
        }
    }
}
