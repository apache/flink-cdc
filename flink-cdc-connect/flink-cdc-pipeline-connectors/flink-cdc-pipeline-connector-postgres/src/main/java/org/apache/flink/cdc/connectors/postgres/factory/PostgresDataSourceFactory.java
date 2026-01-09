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

package org.apache.flink.cdc.connectors.postgres.factory;

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
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresPartitionInclusionDecider;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresPartitionRouter;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.DECODING_PLUGIN_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PARTITION_TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PG_PORT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCHEMA;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SLOT_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLE_ID_INCLUDE_DATABASE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link PostgresDataSource}. */
@Internal
public class PostgresDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataSourceFactory.class);

    public static final String IDENTIFIER = "postgres";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PG_PORT);
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        String slotName = config.get(SLOT_NAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String chunkKeyColumn = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        String tables = config.get(TABLES);
        String partitionTables = config.get(PARTITION_TABLES);

        // Validate: at least one of 'tables' or 'partition.tables' must be configured
        if ((tables == null || tables.trim().isEmpty())
                && (partitionTables == null || partitionTables.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "At least one of 'tables' or 'partition.tables' must be configured.");
        }

        String explicitDatabase = config.get(DATABASE);
        String explicitSchema = config.get(SCHEMA);
        ZoneId serverTimeZone = getServerTimeZone(config);
        String tablesExclude = config.get(TABLES_EXCLUDE);
        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        StartupOptions startupOptions = getStartupOptions(config);

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
        int lsnCommitCheckpointsDelay = config.get(SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        boolean tableIdIncludeDatabase = config.get(TABLE_ID_INCLUDE_DATABASE);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        Map<String, String> configMap = config.toMap();
        boolean includePartitionedTables = config.get(SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED);
        java.util.Properties dbzProps = new java.util.Properties();
        dbzProps.putAll(getDebeziumProperties(configMap));
        // Determine database: prefer explicit option, otherwise infer from tables
        String databaseToUse;
        Optional<String> databaseFromTables = getValidateDatabaseName(tables);
        if (!StringUtils.isNullOrWhitespaceOnly(explicitDatabase)) {
            checkState(
                    isValidPostgresDbName(explicitDatabase),
                    String.format("%s is not a valid PostgreSQL database name", explicitDatabase));
            if (databaseFromTables.isPresent()) {
                checkState(
                        explicitDatabase.equals(databaseFromTables.get()),
                        "The value of option `database` is `%s`, but not all table names have the same or matching database name in `tables` = %s",
                        explicitDatabase,
                        tables);
            }
            databaseToUse = explicitDatabase;
        } else {
            checkState(
                    databaseFromTables.isPresent(),
                    String.format(
                            "Cannot determine database. Please set '%s' or include database in '%s' (format db.schema.table)",
                            DATABASE.key(), TABLES.key()));
            databaseToUse = databaseFromTables.get();
        }

        // Qualify tables/partitionTables/tablesExclude with default schema when provided
        if (!StringUtils.isNullOrWhitespaceOnly(explicitSchema)) {
            checkState(
                    isValidPostgresDbName(explicitSchema),
                    String.format("%s is not a valid PostgreSQL schema name", explicitSchema));
            tables = qualifyWithDefaultSchemaForTables(tables, explicitSchema);
            if (partitionTables != null) {
                partitionTables =
                        qualifyWithDefaultSchemaForPartitions(partitionTables, explicitSchema);
            }
            if (tablesExclude != null) {
                tablesExclude = qualifyWithDefaultSchemaForTables(tablesExclude, explicitSchema);
            }
        }

        PostgresSourceConfigFactory configFactory =
                PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(databaseToUse)
                        .schemaList(".*")
                        .tableList(".*")
                        .username(username)
                        .password(password)
                        .decodingPluginName(pluginName)
                        .slotName(slotName)
                        .serverTimeZone(serverTimeZone.getId())
                        .debeziumProperties(dbzProps)
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .startupOptions(startupOptions)
                        .chunkKeyColumn(chunkKeyColumn)
                        .heartbeatInterval(heartbeatInterval)
                        .closeIdleReaders(closeIdleReaders)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .lsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay)
                        .assignUnboundedChunkFirst(isAssignUnboundedChunkFirst)
                        // Enable enumerator to track and discover newly added tables/partitions
                        .scanNewlyAddedTableEnabled(true)
                        .includePartitionedTables(includePartitionedTables)
                        .partitionTables(partitionTables)
                        .includeDatabaseInTableId(tableIdIncludeDatabase)
                        .getConfigFactory();

        // Always discover tables to validate user patterns, using a temporary wide-open config
        List<TableId> tableIds = PostgresSchemaUtils.listTables(configFactory.create(0), null);
        PostgresPartitionRouter postgresPartitionRouter =
                new PostgresPartitionRouter(includePartitionedTables, tables, partitionTables);
        PostgresPartitionInclusionDecider inclusionDecider =
                new PostgresPartitionInclusionDecider(t -> true, postgresPartitionRouter);
        Selectors selectors = inclusionDecider.getSelectors();
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
        String metadataList = config.get(METADATA_LIST);
        List<PostgreSQLReadableMetadata> readableMetadataList = listReadableMetadata(metadataList);

        return new PostgresDataSource(configFactory, readableMetadataList, postgresPartitionRouter);
    }

    private List<PostgreSQLReadableMetadata> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return new ArrayList<>();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<PostgreSQLReadableMetadata> foundMetadata = new ArrayList<>();
        for (PostgreSQLReadableMetadata metadata : PostgreSQLReadableMetadata.values()) {
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
                        "[%s] cannot be found in postgresSQL metadata.",
                        String.join(", ", readableMetadataList)));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(SLOT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLES);
        options.add(DATABASE);
        options.add(SCHEMA);
        options.add(PG_PORT);
        options.add(TABLES_EXCLUDE);
        options.add(PARTITION_TABLES);
        options.add(DECODING_PLUGIN_NAME);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SERVER_TIME_ZONE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        options.add(METADATA_LIST);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        options.add(SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED);
        options.add(TABLE_ID_INCLUDE_DATABASE);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static List<String> getTableList(
            @Nullable List<TableId> tableIdList, Selectors selectors) {
        List<String> result = new ArrayList<>();
        if (tableIdList == null || tableIdList.isEmpty()) {
            return result;
        }
        for (TableId t : tableIdList) {
            if (selectors.isMatch(t)) {
                result.add(t.toString());
            }
        }
        return result;
    }

    /** Return true if pattern contains an unescaped dot (.) character. */
    private static boolean containsUnescapedDot(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        boolean escaped = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '.') {
                return true;
            }
        }
        return false;
    }

    private static boolean containsEscapedDotBetweenIdentifiers(String s) {
        if (s == null || s.length() < 3) {
            return false;
        }
        for (int i = 0; i + 1 < s.length(); i++) {
            if (s.charAt(i) == '\\' && s.charAt(i + 1) == '.') {
                char prev = i > 0 ? s.charAt(i - 1) : 0;
                char next = i + 2 < s.length() ? s.charAt(i + 2) : 0;
                if (isIdentifierLike(prev) && isIdentifierLike(next)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isIdentifierLike(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '"';
    }

    private static boolean hasSchemaOrNamespaceForTables(String pattern) {
        if (pattern == null) {
            return false;
        }
        // For `tables` and `tables.exclude`, Selectors split by unescaped dots only. For patterns
        // containing escaped dots between identifier-like characters (e.g. "inventory\\.products"
        // to match a literal dot in the table name), we avoid injecting default schema prefixes,
        // because doing so would change matching semantics (table-only vs schema-qualified).
        return containsUnescapedDot(pattern) || containsEscapedDotBetweenIdentifiers(pattern);
    }

    private static boolean hasSchemaOrNamespaceForPartitions(String pattern) {
        if (pattern == null) {
            return false;
        }
        // For `partition.tables`, routing supports both "schema.tableRegex" and
        // "schema\\.tableRegex".
        return pattern.contains("\\.") || containsUnescapedDot(pattern);
    }

    /**
     * Qualify 'tables' patterns with default schema if missing. Comma-separated values.
     *
     * <p>Examples: orders -> public.orders (when defaultSchema=public) schema.orders -> unchanged
     * db.schema.orders -> unchanged
     */
    private static String qualifyWithDefaultSchemaForTables(String tables, String defaultSchema) {
        if (StringUtils.isNullOrWhitespaceOnly(tables)) {
            return tables;
        }
        String[] parts = tables.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (p == null) {
                continue;
            }
            String t = p.trim();
            if (t.isEmpty()) {
                continue;
            }
            // If no schema/namespace present, prefix with default schema
            if (!hasSchemaOrNamespaceForTables(t)) {
                out.add(defaultSchema + "." + t);
            } else {
                out.add(t);
            }
        }
        return String.join(",", out);
    }

    /**
     * Qualify 'partition.tables' entries with default schema when missing.
     *
     * <p>Handles formats: - parent:childRegex - childRegex
     */
    private static String qualifyWithDefaultSchemaForPartitions(
            String partitionTables, String defaultSchema) {
        if (StringUtils.isNullOrWhitespaceOnly(partitionTables)) {
            return partitionTables;
        }
        String[] entries = partitionTables.split(",");
        List<String> out = new ArrayList<>(entries.length);
        for (String e : entries) {
            if (e == null) {
                continue;
            }
            String s = e.trim();
            if (s.isEmpty()) {
                continue;
            }
            int idx = s.indexOf(':');
            if (idx >= 0) {
                String parent = s.substring(0, idx).trim();
                String child = s.substring(idx + 1).trim();
                if (!parent.isEmpty() && !hasSchemaOrNamespaceForPartitions(parent)) {
                    parent = defaultSchema + "." + parent;
                }
                if (!child.isEmpty() && !hasSchemaOrNamespaceForPartitions(child)) {
                    child = defaultSchema + "." + child;
                }
                out.add(parent + ":" + child);
            } else {
                if (!hasSchemaOrNamespaceForPartitions(s)) {
                    out.add(defaultSchema + "." + s);
                } else {
                    out.add(s);
                }
            }
        }
        return String.join(",", out);
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

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_COMMITTED_OFFSET = "committed-offset";

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_COMMITTED_OFFSET:
                return StartupOptions.committed();

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

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
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
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }

    /**
     * Get the database name.
     *
     * @param tables Table name list, format is "db.schema.table,db.schema.table,..." Each table
     *     name consists of three parts separated by ".", which are database name, schema name, and
     *     table name.
     * @return Database name if found, otherwise returns Optional.empty()
     * @throws IllegalArgumentException If the input parameter is null or does not match the
     *     expected format, or if database names are inconsistent.
     */
    private Optional<String> getValidateDatabaseName(String tables) {
        // Input validation
        if (tables == null || tables.trim().isEmpty()) {
            throw new IllegalArgumentException("Parameter tables cannot be null or empty");
        }

        // Split table name list
        String[] tableNames = tables.split(",");
        String dbName = null;

        for (String tableName : tableNames) {
            // Trim whitespace and split table name
            String trimmedTableName = tableName.trim();
            if (!trimmedTableName.contains(".")) {
                continue; // Skip table names that do not match the expected format
            }

            List<String> tableNameParts = splitByUnescapedDot(trimmedTableName);
            // Only patterns with an explicit database prefix participate in database inference.
            // Supported formats:
            //  - db.schema.table
            //  - schema.table
            //  - table
            if (tableNameParts.size() != 3) {
                continue;
            }
            String currentDbName = tableNameParts.get(0).trim();

            checkState(
                    isValidPostgresDbName(currentDbName),
                    String.format("%s is not a valid PostgreSQL database name", currentDbName));
            if (dbName == null) {
                dbName = currentDbName;
            } else {
                checkState(
                        dbName.equals(currentDbName),
                        "The value of option `%s` is `%s`, but not all table names have the same database name",
                        TABLES.key(),
                        String.join(",", tableNames));
            }
        }

        // If no valid table name is found, return Optional.empty()
        return Optional.ofNullable(dbName);
    }

    /** Validate if the database name conforms to PostgreSQL naming conventions. */
    private boolean isValidPostgresDbName(String dbName) {
        // PostgreSQL database name conventions:
        // 1. Length does not exceed 63 characters
        // 2. Can contain letters, numbers, underscores, and dollar signs
        // 3. Must start with a letter or underscore
        if (dbName == null || dbName.length() > 63) {
            return false;
        }
        return dbName.matches("[a-zA-Z_][a-zA-Z0-9_$]*");
    }

    private static List<String> splitByUnescapedDot(String identifier) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (escaped) {
                current.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                current.append(c);
                escaped = true;
                continue;
            }
            if (c == '.') {
                parts.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        parts.add(current.toString());
        return parts;
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
