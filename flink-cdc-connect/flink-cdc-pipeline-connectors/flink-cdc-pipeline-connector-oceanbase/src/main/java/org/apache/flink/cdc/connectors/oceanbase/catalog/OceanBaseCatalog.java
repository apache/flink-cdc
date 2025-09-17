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

import org.apache.flink.cdc.common.types.DataType;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/** A {@link OceanBaseCatalog} for OceanBase connector that supports schema evolution. */
public abstract class OceanBaseCatalog implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseCatalog.class);

    protected OceanBaseConnectionProvider connectionProvider;
    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseCatalog(OceanBaseConnectorOptions connectorOptions) {
        assert Objects.nonNull(connectorOptions);
        this.connectorOptions = connectorOptions;
    }

    public void open() {
        this.connectionProvider = new OceanBaseConnectionProvider(connectorOptions);
        LOG.info("Open OceanBase catalog");
    }

    protected List<String> executeSingleColumnStatement(String sql, Object... params)
            throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
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

    public abstract boolean tableExists(String databaseName, String tableName)
            throws OceanBaseCatalogException;

    public abstract void createTable(OceanBaseTable table, boolean ignoreIfExists)
            throws OceanBaseCatalogException;

    public abstract void alterAddColumns(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns);

    public abstract void alterDropColumns(
            String schemaName, String tableName, List<String> dropColumns);

    public abstract void alterColumnType(
            String schemaName, String tableName, String columnName, DataType dataType);

    public abstract void renameColumn(
            String schemaName, String tableName, String oldColumnName, String newColumnName);

    public abstract void dropTable(String schemaName, String tableName);

    public abstract void truncateTable(String schemaName, String tableName);

    public void close() {
        LOG.info("Close OceanBase catalog");
    }
}
