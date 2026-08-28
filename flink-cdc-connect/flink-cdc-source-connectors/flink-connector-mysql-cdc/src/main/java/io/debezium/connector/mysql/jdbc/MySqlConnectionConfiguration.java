/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectionConfiguration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

import java.util.Properties;

/**
 * Copied from Debezium project(2.7.4.Final).
 *
 * <p>Change 1: add the {@link #MySqlConnectionConfiguration(Configuration, Properties)}
 * constructor, which merges user-supplied jdbc properties over {@link #DEFAULT_JDBC_PROPERTIES} and
 * builds the url pattern from the result.
 *
 * <p>Change 2: override {@link #factory()} and {@link #getUrlPattern()} to use that pattern rather
 * than the fixed {@code URL_PATTERN} of the base class — the base builds its factory inside its own
 * constructor, before this class's fields are assigned, so {@code createFactory} is left as-is.
 *
 * <p>Change 3: override {@link #getJdbcConfiguration(Configuration)} to re-add the {@code useSSL}
 * and {@code connectTimeout} jdbc properties. The base class resolves {@code connectTimeout} from
 * the {@code database.}-prefixed subset only, while Flink CDC passes {@code connect.timeout.ms} at
 * the top level, and it dropped {@code useSSL} in favour of {@code sslMode} — Flink CDC keeps
 * emitting {@code useSSL} so that user-supplied jdbc properties can still override it.
 */
public class MySqlConnectionConfiguration extends BinlogConnectionConfiguration {

    private static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";

    private static final String JDBC_URL_PATTERN =
            "${protocol}://${hostname}:${port}/?useSSL=${useSSL}&connectTimeout=${connectTimeout}";

    private static final String JDBC_URL_PATTERN_WITH_CUSTOM_USE_SSL =
            "${protocol}://${hostname}:${port}/?connectTimeout=${connectTimeout}";

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();

    private final String flinkUrlPattern;
    private final JdbcConnection.ConnectionFactory flinkFactory;
    public static final String URL_PATTERN =
            "${protocol}://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    public MySqlConnectionConfiguration(Configuration config) {
        this(config, new Properties());
    }

    public MySqlConnectionConfiguration(Configuration config, Properties jdbcProperties) {
        super(config);
        this.flinkUrlPattern = formatJdbcUrl(jdbcProperties);
        this.flinkFactory =
                JdbcConnection.patternBasedFactory(
                        flinkUrlPattern,
                        config.getString(MySqlConnectorConfig.JDBC_DRIVER),
                        getClass().getClassLoader(),
                        MySqlConnectorConfig.JDBC_PROTOCOL);
    }

    @Override
    public JdbcConnection.ConnectionFactory factory() {
        return flinkFactory;
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

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_CONNECTION_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration dbConfig) {
        // Debezium by default expects time zoned data delivered in server timezone
        return Strings.defaultIfBlank(
                dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE), "SERVER");
    }

    @Override
    protected JdbcConfiguration getJdbcConfiguration(Configuration configuration) {
        return JdbcConfiguration.adapt(
                super.getJdbcConfiguration(configuration)
                        .edit()
                        .with(
                                "connectTimeout",
                                Long.toString(getConnectionTimeout(originalConfig()).toMillis()))
                        .with("useSSL", Boolean.toString(sslModeEnabled()))
                        .build());
    }

    @Override
    protected Configuration.Builder getDatabaseConfiguration(Configuration configuration) {
        Configuration.Builder builder = super.getDatabaseConfiguration(configuration);
        builder.withDefault(
                MySqlConnectorConfig.JDBC_PROTOCOL,
                MySqlConnectorConfig.JDBC_PROTOCOL.defaultValue());
        return builder;
    }

    @Override
    public String getUrlPattern() {
        return flinkUrlPattern;
    }

    @Override
    protected JdbcConnection.ConnectionFactory createFactory(Configuration configuration) {
        final String driverClassName = configuration.getString(MySqlConnectorConfig.JDBC_DRIVER);
        return JdbcConnection.patternBasedFactory(
                URL_PATTERN,
                driverClassName,
                getClass().getClassLoader(),
                MySqlConnectorConfig.JDBC_PROTOCOL);
    }
}
