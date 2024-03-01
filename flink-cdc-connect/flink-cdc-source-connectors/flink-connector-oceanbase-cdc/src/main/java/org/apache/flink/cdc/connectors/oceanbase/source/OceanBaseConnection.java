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

package org.apache.flink.cdc.connectors.oceanbase.source;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@link JdbcConnection} extension to be used with OceanBase server. */
public class OceanBaseConnection extends JdbcConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseConnection.class);

    private static final String QUOTED_CHARACTER = "`";
    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";
    private static final String OB_URL_PATTERN =
            "jdbc:oceanbase://${hostname}:${port}/?connectTimeout=${connectTimeout}";

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
                factory(jdbcDriver, jdbcProperties, classLoader),
                QUOTED_CHARACTER,
                QUOTED_CHARACTER);
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
                jdbcDriver.toLowerCase().contains("oceanbase") ? OB_URL_PATTERN : MYSQL_URL_PATTERN;
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

    private static JdbcConnection.ConnectionFactory factory(
            String jdbcDriver, Properties jdbcProperties, ClassLoader classLoader) {
        return JdbcConnection.patternBasedFactory(
                formatJdbcUrl(jdbcDriver, jdbcProperties), jdbcDriver, classLoader);
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
     * @return Table list.
     * @throws SQLException If a database access error occurs.
     */
    public List<String> getTables(String dbPattern, String tbPattern) throws SQLException {
        List<String> result = new ArrayList<>();
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
                                    metaData.getTables(dbName, null, null, new String[] {"TABLE"}),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
                }
                break;
            case "oracle":
                dbNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(null, dbName, null, new String[] {"TABLE"}),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
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
}
