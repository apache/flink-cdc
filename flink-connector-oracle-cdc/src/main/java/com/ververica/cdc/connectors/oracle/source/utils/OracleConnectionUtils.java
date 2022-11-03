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

package com.ververica.cdc.connectors.oracle.source.utils;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import oracle.jdbc.OracleTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/** Oracle connection Utilities. */
public class OracleConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectionUtils.class);

    /** Returned by column metadata in Oracle if no scale is set. */
    private static final int ORACLE_UNSET_SCALE = -127;

    /** show current scn sql in oracle. */
    private static final String SHOW_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";

    /** Creates a new {@link OracleConnection}, but not open the connection. */
    public static OracleConnection createOracleConnection(Configuration dbzConfiguration) {
        Configuration configuration = dbzConfiguration.subset(DATABASE_CONFIG_PREFIX, true);
        return new OracleConnection(
                configuration.isEmpty() ? dbzConfiguration : configuration,
                OracleConnectionUtils.class::getClassLoader);
    }

    /** Fetch current redoLog offsets in Oracle Server. */
    public static RedoLogOffset currentRedoLogOffset(JdbcConnection jdbc) {
        try {
            return jdbc.queryAndMap(
                    SHOW_CURRENT_SCN,
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            return new RedoLogOffset(Scn.valueOf(scn).longValue());
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the scn via '"
                                            + SHOW_CURRENT_SCN
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the redo log position via '"
                            + SHOW_CURRENT_SCN
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    public static List<TableId> listTables(
            JdbcConnection jdbcConnection, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX')";
        try {
            jdbcConnection.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId =
                                    new TableId(jdbcConnection.database(), schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                LOG.info("\t including '{}' for further processing", tableId);
            } else {
                LOG.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }

    /**
     * overwrite table catalog in database schema.
     *
     * @param databaseSchema
     * @param connectorConfig
     */
    public static void overwriteCatalog(
            OracleDatabaseSchema databaseSchema, OracleConnectorConfig connectorConfig) {
        Tables tables = databaseSchema.getTables();
        Set<TableId> tableIds = databaseSchema.tableIds();
        Tables.TableFilter tableFilter = connectorConfig.getTableFilters().dataCollectionFilter();

        for (TableId tableId : tableIds) {
            TableId tableIdWithCatalog =
                    new TableId(
                            connectorConfig.getCatalogName(), tableId.schema(), tableId.table());
            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                overrideOracleSpecificColumnTypes(tables, tableId, tableIdWithCatalog);
                databaseSchema.refresh(tables.forTable(tableIdWithCatalog));
            }
        }
    }

    private static void overrideOracleSpecificColumnTypes(
            Tables tables, TableId tableId, TableId tableIdWithCatalog) {
        TableEditor editor = tables.editTable(tableId);
        editor.tableId(tableIdWithCatalog);

        List<String> columnNames = new ArrayList<>(editor.columnNames());
        for (String columnName : columnNames) {
            Column column = editor.columnWithName(columnName);
            if (column.jdbcType() == Types.TIMESTAMP) {
                editor.addColumn(
                        column.edit()
                                .length(column.scale().orElse(Column.UNSET_INT_VALUE))
                                .scale(null)
                                .create());
            }
            // NUMBER columns without scale value have it set to -127 instead of null;
            // let's rectify that
            else if (column.jdbcType() == OracleTypes.NUMBER) {
                column.scale()
                        .filter(s -> s == ORACLE_UNSET_SCALE)
                        .ifPresent(
                                s -> {
                                    editor.addColumn(column.edit().scale(null).create());
                                });
            }
        }
        tables.overwriteTable(editor.create());
    }
}
