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

package org.apache.flink.cdc.connectors.mysql.factory;

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
import org.apache.flink.cdc.connectors.mysql.source.MySqlDataSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.ServerIdRange;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.utils.MySqlSchemaUtils;
import org.apache.flink.cdc.connectors.mysql.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;

import com.mysql.cj.conf.PropertyKey;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.INCLUDE_COMMENTS_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_ID;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TREAT_TINYINT1_AS_BOOLEAN_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USE_LEGACY_JSON_FORMAT;
import static org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.getJdbcProperties;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link MySqlDataSource}. */
@Internal
public class MySqlDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDataSourceFactory.class);

    public static final String IDENTIFIER = "mysql";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String tables = config.get(TABLES);
        String tablesExclude = config.get(TABLES_EXCLUDE);

        String serverId = validateAndGetServerId(config);
        ZoneId serverTimeZone = getServerTimeZone(config);
        StartupOptions startupOptions = getStartupOptions(config);

        boolean includeSchemaChanges = config.get(SCHEMA_CHANGE_ENABLED);

        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean includeComments = config.get(INCLUDE_COMMENTS_ENABLED);
        boolean treatTinyInt1AsBoolean = config.get(TREAT_TINYINT1_AS_BOOLEAN_ENABLED);

        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean scanBinlogNewlyAddedTableEnabled =
                config.get(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED);
        boolean isParsingOnLineSchemaChanges = config.get(PARSE_ONLINE_SCHEMA_CHANGES);
        boolean useLegacyJsonFormat = config.get(USE_LEGACY_JSON_FORMAT);
        boolean isAssignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        Map<String, String> configMap = config.toMap();
        OptionUtils.printOptions(IDENTIFIER, config.toMap());
        if (includeComments) {
            // set debezium config 'include.schema.comments' to true
            configMap.put(
                    DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX
                            + RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_COMMENTS.name(),
                    "true");
        }

        if (!treatTinyInt1AsBoolean) {
            // set jdbc config 'tinyInt1isBit' to false
            configMap.put(PROPERTIES_PREFIX + PropertyKey.tinyInt1isBit.getKeyName(), "false");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(".*")
                        .tableList(".*")
                        .startupOptions(startupOptions)
                        .serverId(serverId)
                        .serverTimeZone(serverTimeZone.getId())
                        .fetchSize(fetchSize)
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorLower(distributionFactorLower)
                        .distributionFactorUpper(distributionFactorUpper)
                        .heartbeatInterval(heartbeatInterval)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .closeIdleReaders(closeIdleReaders)
                        .includeSchemaChanges(includeSchemaChanges)
                        .debeziumProperties(getDebeziumProperties(configMap))
                        .jdbcProperties(getJdbcProperties(configMap))
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                        .parseOnLineSchemaChanges(isParsingOnLineSchemaChanges)
                        .treatTinyInt1AsBoolean(treatTinyInt1AsBoolean)
                        .useLegacyJsonFormat(useLegacyJsonFormat)
                        .assignUnboundedChunkFirst(isAssignUnboundedChunkFirst);

        List<TableId> tableIds = MySqlSchemaUtils.listTables(configFactory.createConfig(0), null);

        if (scanBinlogNewlyAddedTableEnabled && scanNewlyAddedTableEnabled) {
            throw new IllegalArgumentException(
                    "If both scan.binlog.newly-added-table.enabled and scan.newly-added-table.enabled are true, data maybe duplicate after restore");
        }

        if (scanBinlogNewlyAddedTableEnabled) {
            String newTables = validateTableAndReturnDebeziumStyle(tables);
            configFactory.tableList(newTables);
            configFactory.excludeTableList(tablesExclude);

        } else {
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
                            "Cannot find any table with by the option 'tables.exclude'  = "
                                    + tablesExclude);
                }
            }
            configFactory.tableList(capturedTables.toArray(new String[0]));
        }

        String chunkKeyColumns = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        if (chunkKeyColumns != null) {
            Map<ObjectPath, String> chunkKeyColumnMap = new HashMap<>();

            for (String chunkKeyColumn : chunkKeyColumns.split(";")) {
                String[] splits = chunkKeyColumn.split(":");
                if (splits.length == 2) {
                    Selectors chunkKeySelector =
                            new Selectors.SelectorsBuilder().includeTables(splits[0]).build();
                    List<ObjectPath> tableList =
                            getChunkKeyColumnTableList(tableIds, chunkKeySelector);
                    for (ObjectPath table : tableList) {
                        chunkKeyColumnMap.put(table, splits[1]);
                    }
                } else {
                    throw new IllegalArgumentException(
                            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key()
                                    + " = "
                                    + chunkKeyColumns
                                    + " failed to be parsed in this part '"
                                    + chunkKeyColumn
                                    + "'.");
                }
            }
            LOG.info("Add chunkKeyColumn {}.", chunkKeyColumnMap);
            configFactory.chunkKeyColumn(chunkKeyColumnMap);
        }
        String metadataList = config.get(METADATA_LIST);
        List<MySqlReadableMetadata> readableMetadataList = listReadableMetadata(metadataList);
        return new MySqlDataSource(configFactory, readableMetadataList);
    }

    private List<MySqlReadableMetadata> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return new ArrayList<>();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<MySqlReadableMetadata> foundMetadata = new ArrayList<>();
        for (MySqlReadableMetadata metadata : MySqlReadableMetadata.values()) {
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
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(TABLES_EXCLUDE);
        options.add(SCHEMA_CHANGE_ENABLED);
        options.add(SERVER_ID);
        options.add(SERVER_TIME_ZONE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED);
        options.add(METADATA_LIST);
        options.add(INCLUDE_COMMENTS_ENABLED);
        options.add(USE_LEGACY_JSON_FORMAT);
        options.add(TREAT_TINYINT1_AS_BOOLEAN_ENABLED);
        options.add(PARSE_ONLINE_SCHEMA_CHANGES);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
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

    private static List<ObjectPath> getChunkKeyColumnTableList(
            List<TableId> tableIds, Selectors selectors) {
        return tableIds.stream()
                .filter(selectors::isMatch)
                .map(tableId -> new ObjectPath(tableId.getSchemaName(), tableId.getTableName()))
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

            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                validateSpecificOffset(config);
                return getSpecificOffset(config);

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

    private static void validateSpecificOffset(Configuration config) {
        Optional<String> gtidSet = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        Optional<String> binlogFilename = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Optional<Long> binlogPosition = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        if (!gtidSet.isPresent() && !(binlogFilename.isPresent() && binlogPosition.isPresent())) {
            throw new ValidationException(
                    String.format(
                            "Unable to find a valid binlog offset. Either %s, or %s and %s are required.",
                            SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET.key(),
                            SCAN_STARTUP_SPECIFIC_OFFSET_FILE.key(),
                            SCAN_STARTUP_SPECIFIC_OFFSET_POS.key()));
        }
    }

    private static StartupOptions getSpecificOffset(Configuration config) {
        BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();

        // GTID set
        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                .ifPresent(offsetBuilder::setGtidSet);

        // Binlog file + pos
        Optional<String> binlogFilename = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Optional<Long> binlogPosition = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        if (binlogFilename.isPresent() && binlogPosition.isPresent()) {
            offsetBuilder.setBinlogFilePosition(binlogFilename.get(), binlogPosition.get());
        } else {
            offsetBuilder.setBinlogFilePosition("", 0);
        }

        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                .ifPresent(offsetBuilder::setSkipEvents);
        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                .ifPresent(offsetBuilder::setSkipRows);
        return StartupOptions.specificOffset(offsetBuilder.build());
    }

    private String validateAndGetServerId(Configuration configuration) {
        final String serverIdValue = configuration.get(SERVER_ID);
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

    /**
     * Currently, The supported regular syntax is not exactly the same in {@link Selectors} and
     * {@link Tables.TableFilter}.
     *
     * <p>The main distinction are :
     *
     * <p>1) {@link Selectors} use `,` to split table names and {@link Tables.TableFilter} use use
     * `|` to split table names.
     *
     * <p>2) If there is a need to use a dot (.) in a regular expression to match any character, it
     * is necessary to escape the dot with a backslash, refer to {@link
     * MySqlDataSourceOptions#TABLES}.
     */
    private String validateTableAndReturnDebeziumStyle(String tables) {
        // MySQL table names are not allowed to have `,` character.
        if (tables.contains(",")) {
            throw new IllegalArgumentException(
                    "the `,` in "
                            + tables
                            + " is not supported when "
                            + SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED
                            + " was enabled.");
        }

        return tables.replace("\\.", ".");
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
