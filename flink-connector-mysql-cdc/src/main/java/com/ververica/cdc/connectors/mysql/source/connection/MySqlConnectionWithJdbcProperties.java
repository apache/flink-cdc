/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.connection;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;

import java.util.Properties;

/** {@link MySqlConnection} extension to be used with MySQL Server. */
public class MySqlConnectionWithJdbcProperties extends MySqlConnection {
    private final String urlPattern;
    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     */
    public MySqlConnectionWithJdbcProperties(
            MySqlConnectionConfigurationWithCustomUrl connectionConfig) {
        super(connectionConfig);
        this.urlPattern = connectionConfig.getUrlPattern();
    }

    @Override
    public String connectionString() {
        return connectionString(urlPattern);
    }

    /**
     * {@link MySqlConnectionConfiguration} extension to be used with {@link
     * MySqlConnectionWithJdbcProperties}.
     */
    public static class MySqlConnectionConfigurationWithCustomUrl
            extends io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration {
        private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
        private static final String JDBC_URL_PATTERN =
                "jdbc:mysql://${hostname}:${port}/?useSSL=${useSSL}&connectTimeout=${connectTimeout}";
        private static final String JDBC_URL_PATTERN_WITH_CUSTOM_USE_SSL =
                "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";

        private final ConnectionFactory customFactory;
        private final String urlPattern;

        public MySqlConnectionConfigurationWithCustomUrl(
                Configuration config, Properties jdbcProperties) {
            // Set up the JDBC connection without actually connecting, with extra MySQL-specific
            // properties
            // to give us better JDBC database metadata behavior, including using UTF-8 for the
            // client-side character encoding
            // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
            super(config);
            this.urlPattern = formatJdbcUrl(jdbcProperties);
            String driverClassName = config().getString(MySqlConnectorConfig.JDBC_DRIVER);
            customFactory =
                    JdbcConnection.patternBasedFactory(
                            urlPattern, driverClassName, getClass().getClassLoader());
        }

        public String getUrlPattern() {
            return urlPattern;
        }

        private String formatJdbcUrl(Properties jdbcProperties) {
            Properties combinedProperties = new Properties();
            combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
            combinedProperties.putAll(jdbcProperties);

            StringBuilder jdbcUrlStringBuilder =
                    jdbcProperties.getProperty("useSSL") == null
                            ? new StringBuilder(JDBC_URL_PATTERN)
                            : new StringBuilder(JDBC_URL_PATTERN_WITH_CUSTOM_USE_SSL);
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
            defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "CONVERT_TO_NULL");
            defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
            defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
            defaultJdbcProperties.setProperty("useSSL", "false");
            return defaultJdbcProperties;
        }

        @Override
        public ConnectionFactory factory() {
            return customFactory;
        }
    }
}
