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

package org.apache.flink.cdc.connectors.oceanbase.utils;

import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogException;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseMySQLCatalog;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A {@link OceanBaseMySQLCatalog} only for test. */
public class OceanBaseTestMySQLCatalog extends OceanBaseMySQLCatalog {
    public OceanBaseTestMySQLCatalog(OceanBaseConnectorOptions connectorOptions) {
        super(connectorOptions);
        super.open();
    }

    public Optional<OceanBaseTable> getTable(String databaseName, String tableName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");

        final String tableSchemaQuery =
                "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `NUMERIC_SCALE`, `NUMERIC_PRECISION`, "
                        + "`IS_NULLABLE`, `COLUMN_KEY`, `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

        OceanBaseTable.TableType tableType = OceanBaseTable.TableType.UNKNOWN;
        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = connectionProvider.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString("COLUMN_NAME");
                        String type = resultSet.getString("DATA_TYPE");
                        int position = resultSet.getInt("ORDINAL_POSITION");
                        Integer scale = resultSet.getInt("NUMERIC_SCALE");
                        if (resultSet.wasNull()) {
                            scale = null;
                        }
                        Integer precision = null;
                        if ("decimal".equalsIgnoreCase(resultSet.getString("DATA_TYPE"))) {
                            precision = resultSet.getInt("NUMERIC_PRECISION");
                        }
                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String comment = resultSet.getString("COLUMN_COMMENT");
                        OceanBaseColumn column =
                                new OceanBaseColumn.Builder()
                                        .setColumnName(name)
                                        .setOrdinalPosition(position - 1)
                                        .setDataType(type)
                                        .setNumericScale(scale)
                                        .setColumnSize(precision)
                                        .setNullable(
                                                isNullable == null
                                                        || !isNullable.equalsIgnoreCase("NO"))
                                        .setColumnComment(comment.isEmpty() ? null : comment)
                                        .build();
                        columns.add(column);

                        // Only primary key table has value in this field. and the value is "PRI"
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        if (!StringUtils.isNullOrWhitespaceOnly(columnKey)) {
                            if (columnKey.equalsIgnoreCase("PRI")
                                    && tableType == OceanBaseTable.TableType.UNKNOWN) {
                                tableType = OceanBaseTable.TableType.PRIMARY_KEY;
                            }
                            tableKeys.add(column.getColumnName());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new OceanBaseCatalogException(
                    String.format("Failed to get table %s.%s", databaseName, tableName), e);
        }

        OceanBaseTable oceanBaseTable = null;
        if (!columns.isEmpty()) {
            oceanBaseTable =
                    new OceanBaseTable.Builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .setTableType(tableType)
                            .setColumns(columns)
                            .setTableKeys(tableKeys)
                            .build();
        }
        return Optional.ofNullable(oceanBaseTable);
    }

    @Override
    public List<String> executeSingleColumnStatement(String sql, Object... params)
            throws SQLException {
        return super.executeSingleColumnStatement(sql, params);
    }

    @Override
    public void executeUpdateStatement(String sql) throws SQLException {
        super.executeUpdateStatement(sql);
    }
}
