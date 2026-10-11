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

package org.apache.flink.cdc.connectors.dws.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** Opt-in, ownership-scoped environment for tests that require a real DWS cluster. */
public final class DwsNativeTestEnvironment implements AutoCloseable {

    public static final String JDBC_URL_ENV = "DWS_TEST_JDBC_URL";
    public static final String USERNAME_ENV = "DWS_TEST_USERNAME";
    public static final String PASSWORD_ENV = "DWS_TEST_PASSWORD";
    public static final String SCHEMA_ENV = "DWS_TEST_SCHEMA";

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final String TABLE_PREFIX = "flink_cdc_dws_";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String schema;
    private final String ownershipPrefix;
    private final Set<String> ownedTables = new LinkedHashSet<>();

    private DwsNativeTestEnvironment(
            String jdbcUrl, String username, String password, String schema, String runId) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.schema = requireSafeIdentifier(SCHEMA_ENV, schema);
        this.ownershipPrefix = TABLE_PREFIX + requireSafeIdentifier("runId", runId) + "_";
    }

    public static DwsNativeTestEnvironment fromEnvironment() {
        return from(System.getenv());
    }

    static DwsNativeTestEnvironment from(Map<String, String> environment) {
        String runId = UUID.randomUUID().toString().replace("-", "");
        return from(environment, runId);
    }

    static DwsNativeTestEnvironment from(Map<String, String> environment, String runId) {
        return new DwsNativeTestEnvironment(
                requireSetting(environment, JDBC_URL_ENV),
                requireSetting(environment, USERNAME_ENV),
                requireSetting(environment, PASSWORD_ENV),
                requireSetting(environment, SCHEMA_ENV),
                runId);
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String schema() {
        return schema;
    }

    public String ownedTable(String logicalName) {
        String table = ownershipPrefix + requireSafeIdentifier("logical table name", logicalName);
        String qualifiedTable = schema + "." + table;
        ownedTables.add(qualifiedTable);
        return qualifiedTable;
    }

    public boolean isOwnedTable(String qualifiedTable) {
        return qualifiedTable != null && qualifiedTable.startsWith(schema + "." + ownershipPrefix);
    }

    public Set<String> registeredOwnedTables() {
        return Collections.unmodifiableSet(ownedTables);
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public void close() throws SQLException {
        SQLException failure = null;
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            for (String qualifiedTable : ownedTables) {
                try {
                    statement.execute(
                            "DROP TABLE IF EXISTS " + quoteQualifiedTable(qualifiedTable));
                } catch (SQLException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private String quoteQualifiedTable(String qualifiedTable) {
        if (!isOwnedTable(qualifiedTable)) {
            throw new IllegalArgumentException(
                    "Refusing to clean a table not owned by this fixture");
        }
        int separator = qualifiedTable.indexOf('.');
        return '"'
                + qualifiedTable.substring(0, separator)
                + "\".\""
                + qualifiedTable.substring(separator + 1)
                + '"';
    }

    private static String requireSetting(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required external DWS test setting: " + key);
        }
        return value;
    }

    private static String requireSafeIdentifier(String label, String value) {
        if (value == null || !SAFE_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(label + " must be a simple SQL identifier");
        }
        return value;
    }

    @Override
    public String toString() {
        return "DwsNativeTestEnvironment{schema='"
                + schema
                + "', ownershipPrefix='"
                + ownershipPrefix
                + "'}";
    }
}
