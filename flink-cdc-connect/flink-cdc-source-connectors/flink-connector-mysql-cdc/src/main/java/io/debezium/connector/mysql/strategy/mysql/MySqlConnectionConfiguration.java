/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.strategy.mysql;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractConnectionConfiguration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.util.Properties;

/**
 * Copied from Debezium project(2.5.4.Final) to add custom jdbc properties in the jdbc url.
 *
 * <p>Line 20: added the {@link #MySqlConnectionConfiguration(Configuration, Properties)}
 * constructor, which takes the user-supplied jdbc properties.
 *
 * <p>Line 21: added the {@link #urlPattern} field, built from {@link #DEFAULT_JDBC_PROPERTIES}
 * overridden by the user-supplied properties, and exposed through {@link #getUrlPattern()}.
 *
 * <p>Line 22: overrode {@link #factory()} so the connection is created from {@link #urlPattern}
 * instead of the fixed {@link AbstractConnectionConfiguration#URL_PATTERN} of the base class.
 *
 * <p>Line 23: kept the {@code useSSL} jdbc property. Debezium dropped it in favour of {@code
 * sslMode}, but Flink CDC has always emitted {@code useSSL} in the jdbc url and users override it
 * through {@code jdbc.properties.useSSL}; {@link #config()} adds it so the url pattern can
 * reference it.
 */
public class MySqlConnectionConfiguration extends AbstractConnectionConfiguration {

    private static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";

    private static final String JDBC_URL_PATTERN =
            "${protocol}://${hostname}:${port}/?useSSL=${useSSL}&connectTimeout=${connectTimeout}";

    private static final String JDBC_URL_PATTERN_WITH_CUSTOM_USE_SSL =
            "${protocol}://${hostname}:${port}/?connectTimeout=${connectTimeout}";

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();

    private final String urlPattern;
    private final JdbcConnection.ConnectionFactory factory;

    public MySqlConnectionConfiguration(Configuration config) {
        this(config, new Properties());
    }

    public MySqlConnectionConfiguration(Configuration config, Properties jdbcProperties) {
        super(config);
        this.urlPattern = formatJdbcUrl(jdbcProperties);
        final String driverClassName = config.getString(MySqlConnectorConfig.JDBC_DRIVER);
        final Field protocol = MySqlConnectorConfig.JDBC_PROTOCOL;
        this.factory =
                JdbcConnection.patternBasedFactory(
                        urlPattern, driverClassName, getClass().getClassLoader(), protocol);
    }

    @Override
    public JdbcConnection.ConnectionFactory factory() {
        return factory;
    }

    @Override
    public JdbcConfiguration config() {
        return JdbcConfiguration.adapt(
                super.config().edit().with("useSSL", Boolean.toString(sslModeEnabled())).build());
    }

    public String getUrlPattern() {
        return urlPattern;
    }

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_CONNECTION_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration dbConfig) {
        // Debezium by default expects time zoned data delivered in server timezone
        String connectionTimeZone = dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE);
        return connectionTimeZone != null ? connectionTimeZone : "SERVER";
    }

    private String formatJdbcUrl(Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        combinedProperties.putAll(jdbcProperties);

        // when the user supplies their own useSSL, drop the one baked into the pattern so that it
        // is not emitted twice
        StringBuilder jdbcUrlStringBuilder =
                jdbcProperties.getProperty("useSSL") == null
                        ? new StringBuilder(JDBC_URL_PATTERN)
                        : new StringBuilder(JDBC_URL_PATTERN_WITH_CUSTOM_USE_SSL);
        combinedProperties.forEach(
                (key, value) ->
                        jdbcUrlStringBuilder.append("&").append(key).append("=").append(value));
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
        return defaultJdbcProperties;
    }
}
