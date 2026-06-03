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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.connectors.tdengine.utils.TDengineColumnDescription;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Default JDBC client wrapper for TDengine. */
class DefaultTDengineClientWrapper implements TDengineClientWrapper {

    private final Connection connection;

    DefaultTDengineClientWrapper(TDengineDataSinkConfig config) throws SQLException {
        registerDriver(config.getUrl());
        Properties properties = new Properties();
        properties.setProperty("user", config.getUsername());
        properties.setProperty("password", config.getPassword());
        properties.putAll(config.getConnectionProperties());
        this.connection = DriverManager.getConnection(config.getUrl(), properties);
    }

    @Override
    public void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public List<TDengineColumnDescription> describeStable(TDengineTableInfo tableInfo)
            throws SQLException {
        String sql = "DESCRIBE " + tableInfo.qualifiedStableName();
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<TDengineColumnDescription> columns = new ArrayList<>();
            while (resultSet.next()) {
                String note = readColumn(resultSet, "note", "Note");
                columns.add(
                        new TDengineColumnDescription(
                                readColumn(resultSet, "field", "Field"),
                                readColumn(resultSet, "type", "Type"),
                                note != null && note.toUpperCase(Locale.ROOT).contains("TAG")));
            }
            return columns;
        }
    }

    @Override
    public boolean isValid() throws SQLException {
        return !connection.isClosed() && connection.isValid(2);
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    private static String readColumn(ResultSet resultSet, String... candidates)
            throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int index = 1; index <= metaData.getColumnCount(); index++) {
            String label = metaData.getColumnLabel(index);
            for (String candidate : candidates) {
                if (label.equalsIgnoreCase(candidate)) {
                    return resultSet.getString(index);
                }
            }
        }
        return resultSet.getString(1);
    }

    private static synchronized void registerDriver(String jdbcUrl) throws SQLException {
        try {
            DriverManager.getDriver(jdbcUrl);
            return;
        } catch (SQLException ignored) {
            // Register explicitly below.
        }

        String driverName =
                jdbcUrl.startsWith("jdbc:TAOS-WS://")
                        ? "com.taosdata.jdbc.ws.WebSocketDriver"
                        : "com.taosdata.jdbc.TSDBDriver";
        try {
            Class<?> clazz =
                    Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
            Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(driver);
        } catch (ReflectiveOperationException e) {
            throw new SQLException("Failed to register TDengine JDBC driver " + driverName, e);
        }
    }
}
