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

package org.apache.flink.cdc.connectors.tidb.source.connection;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class TiDBConnection extends JdbcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBConnection.class);

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;
    private static final int TYPE_TIMESTAMP_WITH_TIME_ZONE = -101;
    private static final int TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102;
    private static final int TYPE_INTERVAL_YEAR_TO_MONTH = -103;
    private static final int TYPE_INTERVAL_DAY_TO_SECOND = -104;
    private static final char quote = '`';
    private static final String QUOTED_CHARACTER = "`";

    public TiDBConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            Duration timeout,
            String jdbcDriver,
            Properties jdbcProperties,
            ClassLoader classLoader) {
        super(
                config(hostname, port, user, password, timeout),
                JdbcConnection.patternBasedFactory(
                        formatJdbcUrl(jdbcDriver, jdbcProperties), jdbcDriver, classLoader),
                quote + "",
                quote + "");
    }

    public TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            String openingQuoteCharacter,
            String closingQuoteCharacter) {
        super(config, connectionFactory, openingQuoteCharacter, closingQuoteCharacter);
    }

    public TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            Supplier<ClassLoader> classLoaderSupplier,
            String openingQuoteCharacter,
            String closingQuoteCharacter) {
        super(
                config,
                connectionFactory,
                classLoaderSupplier,
                openingQuoteCharacter,
                closingQuoteCharacter);
    }

    protected TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            Operations initialOperations,
            Supplier<ClassLoader> classLoaderSupplier,
            String openingQuotingChar,
            String closingQuotingChar) {
        super(
                config,
                connectionFactory,
                initialOperations,
                classLoaderSupplier,
                openingQuotingChar,
                closingQuotingChar);
    }

    private static JdbcConfiguration config(
            String hostname, Integer port, String user, String password, Duration timeout) {
        return JdbcConfiguration.create()
                .with("hostname", hostname)
                .with("port", port)
                .with("user", user)
                .with("password", password)
                .with("connectTimeout", timeout == null ? 30000 : timeout.toMillis())
                .build();
    }

    private static String formatJdbcUrl(String jdbcDriver, Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        if (jdbcProperties != null) {
            combinedProperties.putAll(jdbcProperties);
        }
        StringBuilder jdbcUrlStringBuilder = new StringBuilder(MYSQL_URL_PATTERN);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });
        return jdbcUrlStringBuilder.toString();
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
        return defaultJdbcProperties;
    }

    public long getCurrentTimestampS() throws SQLException {
        try {
            long globalTimestamp = getGlobalTimestamp();
            LOG.info("Global timestamp: {}", globalTimestamp);
            return Long.parseLong(String.valueOf(globalTimestamp).substring(0, 10));
        } catch (Exception e) {
            LOG.warn("Failed to get global timestamp, use local timestamp instead");
        }
        return getCurrentTimestamp()
                .orElseThrow(IllegalStateException::new)
                .toInstant()
                .getEpochSecond();
    }

    private long getGlobalTimestamp() throws SQLException {
        return querySingleValue(
                connection(), "SELECT CURRENT_TIMESTAMP FROM DUAL", ps -> {}, rs -> rs.getLong(1));
    }

    @Override
    public Optional<Timestamp> getCurrentTimestamp() throws SQLException {
        return queryAndMap(
                "SELECT LOCALTIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1)) : Optional.empty());
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[] {"TABLE"};
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString(quote);
    }

    public void readSchemaForCapturedTables(
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc,
            Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata =
                    metadata.getColumns(
                            databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter)
                            .ifPresent(
                                    column -> {
                                        columnsByTable
                                                .computeIfAbsent(tableId, t -> new ArrayList<>())
                                                .add(column.create());
                                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    @Override
    protected int resolveNativeType(String typeName) {
        String upperCaseTypeName = typeName.toUpperCase();
        if (upperCaseTypeName.startsWith("JSON")) {
            return Types.VARCHAR;
        }
        if (upperCaseTypeName.startsWith("NCHAR")) {
            return Types.NCHAR;
        }
        if (upperCaseTypeName.startsWith("NVARCHAR2")) {
            return Types.NVARCHAR;
        }
        if (upperCaseTypeName.startsWith("TIMESTAMP")) {
            if (upperCaseTypeName.contains("WITH TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_TIME_ZONE;
            }
            if (upperCaseTypeName.contains("WITH LOCAL TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            }
            return Types.TIMESTAMP;
        }
        if (upperCaseTypeName.startsWith("INTERVAL")) {
            if (upperCaseTypeName.contains("TO MONTH")) {
                return TYPE_INTERVAL_YEAR_TO_MONTH;
            }
            if (upperCaseTypeName.contains("TO SECOND")) {
                return TYPE_INTERVAL_DAY_TO_SECOND;
            }
        }
        return Column.UNSET_INT_VALUE;
    }

    public String readSystemVariable(String variable) throws SQLException {
        return querySingleValue(
                connection(),
                "SHOW VARIABLES LIKE ?",
                ps -> ps.setString(1, variable),
                rs -> rs.getString("VALUE"));
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        switch (metadataJdbcType) {
            case TYPE_BINARY_FLOAT:
                return Types.REAL;
            case TYPE_BINARY_DOUBLE:
                return Types.DOUBLE;
            case TYPE_TIMESTAMP_WITH_TIME_ZONE:
            case TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TYPE_INTERVAL_YEAR_TO_MONTH:
            case TYPE_INTERVAL_DAY_TO_SECOND:
                return Types.OTHER;
            default:
                return nativeType == Column.UNSET_INT_VALUE ? metadataJdbcType : nativeType;
        }
    }

    public List<TableId> getTables(String dbPattern, String tbPattern) throws SQLException {
        return listTables(
                db -> Pattern.matches(dbPattern, db),
                tableId -> Pattern.matches(tbPattern, tableId.table()));
    }

    private List<TableId> listTables(
            Predicate<String> databaseFilter, Tables.TableFilter tableFilter) throws SQLException {
        List<TableId> tableIds = new ArrayList<>();
        DatabaseMetaData metaData = connection().getMetaData();
        ResultSet rs = metaData.getCatalogs();
        List<String> dbList = new ArrayList<>();
        while (rs.next()) {
            String db = rs.getString("TABLE_CAT");
            if (databaseFilter.test(db)) {
                dbList.add(db);
            }
        }
        for (String db : dbList) {

            rs = metaData.getTables(db, null, null, supportedTableTypes());
            while (rs.next()) {
                TableId tableId = new TableId(db, null, rs.getString("TABLE_NAME"));
                if (tableFilter.isIncluded(tableId)) {
                    tableIds.add(tableId);
                }
            }
        }
        return tableIds;
    }

    // 新的readSchema
    public void readTiDBSchema(
            TiDBConnectorConfig config,
            TiDBDatabaseSchema databaseSchema,
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.TableFilter tableFilter,
            Tables.ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        // Read the metadata for the table columns ...
        DatabaseMetaData metadata = connection().getMetaData();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> viewIds = new HashSet<>();
        final Set<TableId> tableIds = new HashSet<>();

        int totalTables = 0;
        try (final ResultSet rs =
                metadata.getTables(
                        databaseCatalog, schemaNamePattern, null, supportedTableTypes())) {
            while (rs.next()) {
                final String catalogName = resolveCatalogName(rs.getString(1));
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final String tableType = rs.getString(4);
                if (isTableType(tableType)) {
                    totalTables++;
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                        tableIds.add(tableId);
                    }
                } else {
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    viewIds.add(tableId);
                }
            }
        }

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        if (totalTables == tableIds.size()) {
            columnsByTable =
                    getColumnsDetailsWithTableChange(
                            config,
                            databaseSchema,
                            databaseCatalog,
                            schemaNamePattern,
                            null,
                            tableFilter,
                            columnFilter,
                            metadata,
                            viewIds);
            //            LOGGER.info("connection readSchema:", columnsByTable);
        } else {
            for (TableId includeTable : tableIds) {
                Map<TableId, List<Column>> cols =
                        getColumnsDetailsWithTableChange(
                                config,
                                databaseSchema,
                                databaseCatalog,
                                schemaNamePattern,
                                null,
                                tableFilter,
                                columnFilter,
                                metadata,
                                viewIds);
                columnsByTable.putAll(cols);
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames =
                    readPrimaryKeyOrUniqueIndexNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    protected Map<TableId, List<Column>> getColumnsDetailsWithTableChange(
            TiDBConnectorConfig config,
            TiDBDatabaseSchema databaseSchema,
            String databaseCatalog,
            String schemaNamePattern,
            String tableName,
            Tables.TableFilter tableFilter,
            Tables.ColumnNameFilter columnFilter,
            DatabaseMetaData metadata,
            final Set<TableId> viewIds)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        try (ResultSet columnMetadata =
                metadata.getColumns(databaseCatalog, schemaNamePattern, tableName, null)) {
            while (columnMetadata.next()) {
                String catalogName = resolveCatalogName(columnMetadata.getString(1));
                String schemaName = columnMetadata.getString(2);
                String metaTableName = columnMetadata.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, metaTableName);

                // exclude views and non-captured tables
                if (viewIds.contains(tableId)
                        || (tableFilter != null && !tableFilter.isIncluded(tableId))) {
                    continue;
                }
                TableChanges.TableChange tableChange =
                        readTableSchema(config, databaseSchema, tableId);
                if (tableChange != null) {
                    ArrayList<Column> columns = new ArrayList<>(tableChange.getTable().columns());
                    columnsByTable.put(tableId, columns);
                }
            }
        }
        return columnsByTable;
    }

    private TableChanges.TableChange readTableSchema(
            TiDBConnectorConfig connectorConfig,
            TiDBDatabaseSchema databaseSchema,
            TableId tableId) {
        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
        String showCreateTable = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
        final TiDBPartition partition = new TiDBPartition(connectorConfig.getLogicalName());
        buildSchemaByShowCreateTable(
                connectorConfig, databaseSchema, partition, this, tableId, tableChangeMap);
        return tableChangeMap.get(tableId);
    }

    private void buildSchemaByShowCreateTable(
            TiDBConnectorConfig config,
            TiDBDatabaseSchema databaseSchema,
            TiDBPartition partition,
            JdbcConnection jdbc,
            TableId tableId,
            Map<TableId, TableChanges.TableChange> tableChangeMap) {
        final String sql = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            parseSchemaByDdl(
                                    config,
                                    databaseSchema,
                                    partition,
                                    ddl,
                                    tableId,
                                    tableChangeMap);
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
    }

    private void parseSchemaByDdl(
            TiDBConnectorConfig config,
            TiDBDatabaseSchema databaseSchema,
            TiDBPartition partition,
            String ddl,
            TableId tableId,
            Map<TableId, TableChanges.TableChange> tableChangeMap) {
        final EventOffsetContext offsetContext = EventOffsetContext.initial(config);
        List<SchemaChangeEvent> schemaChangeEvents =
                databaseSchema.parseSnapshotDdl(
                        partition, ddl, tableId.catalog(), offsetContext, Instant.now());
        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
            for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                tableChangeMap.put(tableId, tableChange);
            }
        }
    }
}
