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

package org.apache.flink.cdc.connectors.oceanbase.source.connection;

import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@link JdbcConnection} extension to be used with OceanBase server. */
public class OceanBaseConnection extends JdbcConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseConnection.class);

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";
    private static final String OB_URL_PATTERN =
            "jdbc:oceanbase://${hostname}:${port}/?connectTimeout=${connectTimeout}";

    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;
    private static final int TYPE_TIMESTAMP_WITH_TIME_ZONE = -101;
    private static final int TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102;
    private static final int TYPE_INTERVAL_YEAR_TO_MONTH = -103;
    private static final int TYPE_INTERVAL_DAY_TO_SECOND = -104;

    private final String compatibleMode;

    public OceanBaseConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            Duration timeout,
            String compatibleMode,
            String jdbcDriver,
            Properties jdbcProperties,
            ClassLoader classLoader) {
        super(
                config(hostname, port, user, password, timeout),
                JdbcConnection.patternBasedFactory(
                        formatJdbcUrl(jdbcDriver, jdbcProperties), jdbcDriver, classLoader),
                getQuote(compatibleMode) + "",
                getQuote(compatibleMode) + "");
        this.compatibleMode = compatibleMode;
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
        String urlPattern =
                OceanBaseUtils.isOceanBaseDriver(jdbcDriver) ? OB_URL_PATTERN : MYSQL_URL_PATTERN;
        StringBuilder jdbcUrlStringBuilder = new StringBuilder(urlPattern);
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

    private static char getQuote(String compatibleMode) {
        return "mysql".equalsIgnoreCase(compatibleMode) ? '`' : '"';
    }

    /**
     * Get current timestamp number in seconds.
     *
     * @return current timestamp number.
     * @throws SQLException If a database access error occurs.
     */
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
        String schema = "mysql".equalsIgnoreCase(compatibleMode) ? "oceanbase" : "SYS";
        return querySingleValue(
                connection(),
                "SELECT TS_VALUE FROM " + schema + ".V$OB_TIMESTAMP_SERVICE",
                ps -> {},
                rs -> rs.getLong(1));
    }

    @Override
    public Optional<Timestamp> getCurrentTimestamp() throws SQLException {
        return queryAndMap(
                "mysql".equalsIgnoreCase(compatibleMode)
                        ? "SELECT CURRENT_TIMESTAMP"
                        : "SELECT CURRENT_TIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1)) : Optional.empty());
    }

    /**
     * Get table list by database name pattern and table name pattern.
     *
     * @param dbPattern Database name pattern.
     * @param tbPattern Table name pattern.
     * @return TableId list.
     * @throws SQLException If a database access error occurs.
     */
    public List<TableId> getTables(String dbPattern, String tbPattern) throws SQLException {
        List<TableId> result = new ArrayList<>();
        DatabaseMetaData metaData = connection().getMetaData();
        switch (compatibleMode.toLowerCase()) {
            case "mysql":
                List<String> dbNames = getResultList(metaData.getCatalogs(), "TABLE_CAT");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(dbName, null, null, supportedTableTypes()),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(new TableId(dbName, null, tbName)));
                }
                break;
            case "oracle":
                List<String> schemaNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
                schemaNames =
                        schemaNames.stream()
                                .filter(schemaName -> Pattern.matches(dbPattern, schemaName))
                                .collect(Collectors.toList());
                for (String schemaName : schemaNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(
                                            null, schemaName, null, supportedTableTypes()),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(new TableId(null, schemaName, tbName)));
                }
                break;
            default:
                throw new FlinkRuntimeException("Unsupported compatible mode: " + compatibleMode);
        }
        return result;
    }

    private List<String> getResultList(ResultSet resultSet, String columnName) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(columnName));
        }
        return result;
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[] {"TABLE"};
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString(getQuote(compatibleMode));
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
}
