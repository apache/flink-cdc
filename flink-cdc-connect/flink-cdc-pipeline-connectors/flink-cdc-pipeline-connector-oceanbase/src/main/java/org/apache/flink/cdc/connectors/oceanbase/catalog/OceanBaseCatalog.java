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

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import org.apache.commons.compress.utils.Lists;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A {@link OceanBaseCatalog} for OceanBase connector that supports schema evolution. */
public abstract class OceanBaseCatalog implements Serializable {
    private static final long serialVersionUID = 1L;
    private final OceanBaseConnectionProvider connectionProvider;

    public OceanBaseCatalog(OceanBaseConnectorOptions connectorOptions) {
        assert Objects.nonNull(connectorOptions);
        this.connectionProvider = new OceanBaseConnectionProvider(connectorOptions);
    }

    protected List<String> executeSingleColumnStatement(String sql) throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<String> columnValues = Lists.newArrayList();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(1);
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        }
    }

    protected void executeUpdateStatement(String sql) throws SQLException {
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    public abstract boolean databaseExists(String databaseName) throws OceanBaseCatalogException;

    public abstract void createDatabase(String databaseName, boolean ignoreIfExists)
            throws OceanBaseCatalogException;

    public abstract void createTable(OceanBaseTable table, boolean ignoreIfExists)
            throws OceanBaseCatalogException;

    public abstract void alterAddColumns(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns);

    protected abstract String buildCreateDatabaseSql(String databaseName, boolean ignoreIfExists);

    protected abstract String buildCreateTableSql(OceanBaseTable table, boolean ignoreIfExists);

    protected abstract String buildColumnStmt(OceanBaseColumn column);

    protected abstract String getFullColumnType(
            String type, Optional<Integer> columnSize, Optional<Integer> decimalDigits);

    protected abstract String buildAlterAddColumnsSql(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns);
}
