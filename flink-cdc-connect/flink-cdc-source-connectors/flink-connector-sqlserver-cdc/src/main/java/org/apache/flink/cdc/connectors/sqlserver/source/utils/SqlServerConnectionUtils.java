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

package org.apache.flink.cdc.connectors.sqlserver.source.utils;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Utils for SqlServer connection. */
public class SqlServerConnectionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerConnectionUtils.class);

    public static SqlServerConnection createSqlServerConnection(
            RelationalDatabaseConnectorConfig connectorConfig) {
        Configuration dbzConnectorConfig = connectorConfig.getJdbcConfig();

        final SqlServerValueConverters valueConverters =
                new SqlServerValueConverters(
                        connectorConfig.getDecimalMode(),
                        connectorConfig.getTemporalPrecisionMode(),
                        connectorConfig.binaryHandlingMode());
        return new SqlServerConnection(
                JdbcConfiguration.adapt(dbzConnectorConfig),
                ((SqlServerConnectorConfig) connectorConfig).getSourceTimestampMode(),
                valueConverters,
                SqlServerConnectionUtils.class::getClassLoader,
                connectorConfig.getSkippedOperations(),
                false);
    }

    public static List<TableId> listTables(
            JdbcConnection jdbc, RelationalTableFilters tableFilters, List<String> databaseList)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();

        jdbc.query(
                "SELECT name, database_id, create_date  \n" + "FROM sys.databases;  ",
                rs -> {
                    while (rs.next()) {
                        // Because sqlserver table filter cannot filter by database name, we need to
                        // filter here
                        String databaseName = rs.getString(1);
                        if (databaseList.contains(databaseName)) {
                            databaseNames.add(databaseName);
                        }
                    }
                });
        LOG.info("\t list of available databases is: {}", databaseNames);

        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // SqlServer, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in each database");
        for (String dbName : databaseNames) {
            try {
                jdbc.query(
                        "SELECT * FROM "
                                + SqlServerUtils.quote(dbName)
                                + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';",
                        rs -> {
                            while (rs.next()) {
                                TableId tableId =
                                        new TableId(
                                                rs.getString(1), rs.getString(2), rs.getString(3));
                                if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                    capturedTableIds.add(tableId);
                                    LOG.info("\t including '{}' for further processing", tableId);
                                } else {
                                    LOG.info("\t '{}' is filtered out of capturing", tableId);
                                }
                            }
                        });
            } catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                LOG.warn(
                        "\t skipping database '{}' due to error reading tables: {}",
                        dbName,
                        e.getMessage());
            }
        }
        return capturedTableIds;
    }
}
