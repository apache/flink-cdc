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

package org.apache.flink.cdc.common.source.discover;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A {@link TableDiscoverer} that reads the list of subscribed tables from a JDBC database table.
 *
 * <p>This implementation connects to any JDBC-compatible database (e.g., MySQL, PostgreSQL) and
 * reads table names from a specified column. The table names are parsed as {@link TableId} objects.
 *
 * <p><b>Configuration keys</b> (read from the full connector configuration):
 *
 * <ul>
 *   <li>{@code table.discoverer.jdbc.url} — JDBC connection URL (required).
 *   <li>{@code table.discoverer.jdbc.table-name} — The database table storing subscription entries
 *       (required).
 *   <li>{@code table.discoverer.jdbc.username} — JDBC username (required).
 *   <li>{@code table.discoverer.jdbc.password} — JDBC password (required).
 *   <li>{@code table.discoverer.jdbc.column-name} — The column containing fully-qualified table
 *       names. Defaults to {@code "subscribe_table_name"}.
 * </ul>
 *
 * <p><b>Expected schema:</b> The target column must contain fully-qualified table names formatted
 * as {@code "schemaName.tableName"} (two-part) or {@code "namespace.schemaName.tableName"}
 * (three-part). For example:
 *
 * <pre>{@code
 * CREATE TABLE cdc_subscriptions (
 *     subscribe_table_name VARCHAR(255) PRIMARY KEY
 * );
 * INSERT INTO cdc_subscriptions VALUES ('source_db.orders'), ('source_db.products');
 * }</pre>
 *
 * <p>Null values and rows that cannot be parsed into a valid {@link TableId} are silently skipped.
 */
public class JdbcTableDiscoverer implements TableDiscoverer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTableDiscoverer.class);

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("table.discoverer.jdbc.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC connection URL for the table discovery database.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table.discoverer.jdbc.table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the database table storing the subscription entries.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("table.discoverer.jdbc.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC username for the table discovery database.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("table.discoverer.jdbc.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC password for the table discovery database.");

    public static final ConfigOption<String> COLUMN_NAME =
            ConfigOptions.key("table.discoverer.jdbc.column-name")
                    .stringType()
                    .defaultValue("subscribe_table_name")
                    .withDescription(
                            "The column name in the subscription table that contains the "
                                    + "fully-qualified table names to subscribe to.");

    private transient Connection connection;
    private transient String tableName;
    private transient String columnName;

    @Override
    public void open(Context context) throws Exception {
        Configuration config = context.getConfiguration();

        String jdbcUrl = config.get(JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + JDBC_URL.key() + "' is required for JdbcTableDiscoverer.");
        }
        tableName = config.get(TABLE_NAME);
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + TABLE_NAME.key() + "' is required for JdbcTableDiscoverer.");
        }
        String username = config.get(USERNAME);
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + USERNAME.key() + "' is required for JdbcTableDiscoverer.");
        }
        String password = config.get(PASSWORD);
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + PASSWORD.key() + "' is required for JdbcTableDiscoverer.");
        }
        columnName = config.get(COLUMN_NAME);

        connection = DriverManager.getConnection(jdbcUrl, username, password);
        LOG.info(
                "JdbcTableDiscoverer opened connection to '{}', table='{}', column='{}'.",
                jdbcUrl,
                tableName,
                columnName);
    }

    @Override
    public Set<TableId> discover() throws Exception {
        Set<TableId> result = new LinkedHashSet<>();
        String sql = "SELECT " + columnName + " FROM " + tableName;
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String value = rs.getString(1);
                if (value == null || value.isEmpty()) {
                    continue;
                }
                try {
                    result.add(TableId.parse(value));
                } catch (IllegalArgumentException e) {
                    LOG.warn(
                            "Skipping invalid table name '{}' from subscription table '{}'.",
                            value,
                            tableName);
                }
            }
        }
        LOG.info(
                "JdbcTableDiscoverer discovered {} tables from '{}.{}'.",
                result.size(),
                tableName,
                columnName);
        return result;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("JdbcTableDiscoverer closed JDBC connection.");
        }
    }
}
