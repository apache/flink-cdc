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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

/** A {@link TableDiscoverer} that reads the list of subscribed tables from a JDBC database. */
public class JdbcTableDiscoverer implements TableDiscoverer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTableDiscoverer.class);

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("table.discoverer.jdbc.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC connection URL for the table discovery database.");

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

    public static final ConfigOption<String> SUBSCRIBE_QUERY =
            ConfigOptions.key("table.discoverer.jdbc.subscribe-query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom SELECT statement used to discover subscribed tables. When set, "
                                    + "this takes priority over the shared-table options. Column #1 of "
                                    + "every row must be a fully-qualified table name.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table.discoverer.jdbc.table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The shared subscription table that stores subscription entries for "
                                    + "one or more CDC jobs. Required in shared-table mode.");

    public static final ConfigOption<String> COLUMN_NAME =
            ConfigOptions.key("table.discoverer.jdbc.column-name")
                    .stringType()
                    .defaultValue("subscribe_table_name")
                    .withDescription(
                            "The column name in the subscription table that contains the "
                                    + "fully-qualified table names to subscribe to.");

    public static final ConfigOption<String> SUBSCRIBE_ID_COLUMN =
            ConfigOptions.key("table.discoverer.jdbc.subscribe-id-column")
                    .stringType()
                    .defaultValue("subscribe_id")
                    .withDescription(
                            "The column name in the subscription table that holds the "
                                    + "subscription-set identifier used for filtering.");

    public static final ConfigOption<String> SUBSCRIBE_ID =
            ConfigOptions.key("table.discoverer.jdbc.subscribe-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The current subscription-set identifier. Required in shared-table "
                                    + "mode; rows whose subscribe-id column matches this value are "
                                    + "discovered as subscribed tables.");

    /** Compiled SQL to execute on every {@link #discover()} call. */
    private transient String sql;

    /** When non-null, the discoverer runs in shared-table mode and binds this as parameter #1. */
    private transient String subscribeId;

    private transient Connection connection;

    @Override
    public void open(Context context) throws Exception {
        Configuration config = context.getConfiguration();
        ClassLoader userCodeClassLoader = context.getUserCodeClassLoader();

        String jdbcUrl = requireNonEmpty(config, JDBC_URL);
        String username = requireNonEmpty(config, USERNAME);
        String password = requireNonEmpty(config, PASSWORD);

        String subscribeQuery = config.get(SUBSCRIBE_QUERY);
        if (subscribeQuery != null && !subscribeQuery.isEmpty()) {
            // Mode A — custom query takes priority. Filter options are intentionally ignored.
            this.sql = subscribeQuery;
            this.subscribeId = null;
            LOG.info(
                    "JdbcTableDiscoverer running in custom-query mode. URL='{}', query='{}'.",
                    jdbcUrl,
                    subscribeQuery);
        } else {
            // Mode B — shared-table filter; subscribe-id is mandatory.
            String tableName = requireNonEmpty(config, TABLE_NAME);
            String columnName = config.get(COLUMN_NAME);
            String subscribeIdColumn = config.get(SUBSCRIBE_ID_COLUMN);
            this.subscribeId = requireNonEmpty(config, SUBSCRIBE_ID);
            this.sql =
                    "SELECT "
                            + columnName
                            + " FROM "
                            + tableName
                            + " WHERE "
                            + subscribeIdColumn
                            + " = ?";
            LOG.info(
                    "JdbcTableDiscoverer running in shared-table mode. URL='{}', table='{}', "
                            + "column='{}', subscribeIdColumn='{}', subscribeId='{}'.",
                    jdbcUrl,
                    tableName,
                    columnName,
                    subscribeIdColumn,
                    subscribeId);
        }

        connection = openConnection(jdbcUrl, username, password, userCodeClassLoader);
    }

    private static Connection openConnection(
            String jdbcUrl, String username, String password, ClassLoader userCodeClassLoader)
            throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);

        Driver driver = findDriver(jdbcUrl, userCodeClassLoader);
        if (driver != null) {
            Connection connection = driver.connect(jdbcUrl, properties);
            if (connection != null) {
                return connection;
            }
        }
        // Fall back to DriverManager for drivers that are already visible on this class'
        // classloader (e.g. bundled directly with flink-cdc-common).
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private static Driver findDriver(String jdbcUrl, ClassLoader classLoader) throws SQLException {
        for (Driver driver : ServiceLoader.load(Driver.class, classLoader)) {
            if (driver.acceptsURL(jdbcUrl)) {
                return driver;
            }
        }
        return null;
    }

    @Override
    public Set<TableId> discover() throws Exception {
        Set<TableId> result = new LinkedHashSet<>();
        if (subscribeId != null) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, subscribeId);
                try (ResultSet rs = ps.executeQuery()) {
                    collect(rs, result);
                }
            }
        } else {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                collect(rs, result);
            }
        }
        LOG.info("JdbcTableDiscoverer discovered {} tables.", result.size());
        return result;
    }

    private void collect(ResultSet rs, Set<TableId> result) throws Exception {
        while (rs.next()) {
            String value = rs.getString(1);
            if (value == null || value.isEmpty()) {
                continue;
            }
            try {
                result.add(TableId.parse(value));
            } catch (IllegalArgumentException e) {
                LOG.warn(
                        "Skipping invalid table name '{}' returned by JdbcTableDiscoverer.", value);
            }
        }
    }

    private static String requireNonEmpty(Configuration config, ConfigOption<String> option) {
        String value = config.get(option);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + option.key() + "' is required for JdbcTableDiscoverer.");
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("JdbcTableDiscoverer closed JDBC connection.");
        }
    }
}
