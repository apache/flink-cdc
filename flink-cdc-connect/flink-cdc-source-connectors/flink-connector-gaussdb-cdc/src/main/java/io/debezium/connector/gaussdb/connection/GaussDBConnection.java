/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb.connection;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

/** {@link JdbcConnection} extension used for connecting to GaussDB instances via JDBC. */
public class GaussDBConnection extends JdbcConnection {

    public static final String CONNECTION_GENERAL = "Debezium General";

    public static final String DRIVER_CLASS_NAME = "com.huawei.gaussdb.jdbc.Driver";

    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_CONNECT_MAX_RETRIES = 3;
    public static final Duration DEFAULT_CONNECT_RETRY_DELAY = Duration.ofSeconds(5);

    /**
     * Internal connection settings (stored in {@link JdbcConfiguration}) that should not be passed
     * to the JDBC driver.
     */
    public static final String CONNECT_MAX_RETRIES_KEY = "gaussdb.connect.max.retries";

    public static final String CONNECT_RETRY_DELAY_MS_KEY = "gaussdb.connect.retry.delay.ms";

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBConnection.class);

    private static final String URL_PATTERN =
            "jdbc:gaussdb://${"
                    + JdbcConfiguration.HOSTNAME
                    + "}:${"
                    + JdbcConfiguration.PORT
                    + "}/${"
                    + JdbcConfiguration.DATABASE
                    + "}";

    private static final ConnectionFactory DEFAULT_DELEGATE_FACTORY =
            JdbcConnection.patternBasedFactory(
                    URL_PATTERN,
                    DRIVER_CLASS_NAME,
                    GaussDBConnection.class.getClassLoader(),
                    JdbcConfiguration.PORT.withDefault(
                            GaussDBConnectorConfig.PORT.defaultValueAsString()));

    public GaussDBConnection(JdbcConfiguration config, String connectionUsage) {
        this(config, connectionUsage, DEFAULT_DELEGATE_FACTORY);
    }

    @VisibleForTesting
    GaussDBConnection(
            JdbcConfiguration config, String connectionUsage, ConnectionFactory delegateFactory) {
        super(
                addDefaultSettings(config, connectionUsage),
                new RetryingConnectionFactory(delegateFactory),
                "\"",
                "\"");
    }

    static JdbcConfiguration addDefaultSettings(
            JdbcConfiguration configuration, String connectionUsage) {
        return JdbcConfiguration.adapt(
                configuration
                        .edit()
                        .withDefault(
                                "ApplicationName",
                                connectionUsage != null ? connectionUsage : CONNECTION_GENERAL)
                        .withDefault(
                                JdbcConfiguration.CONNECTION_TIMEOUT_MS.name(),
                                String.valueOf(DEFAULT_CONNECTION_TIMEOUT.toMillis()))
                        .withDefault(
                                CONNECT_MAX_RETRIES_KEY,
                                String.valueOf(DEFAULT_CONNECT_MAX_RETRIES))
                        .withDefault(
                                CONNECT_RETRY_DELAY_MS_KEY,
                                String.valueOf(DEFAULT_CONNECT_RETRY_DELAY.toMillis()))
                        .build());
    }

    /** Returns a JDBC connection string for the current configuration. */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /** Executes a lightweight health check query. */
    public void executeHealthCheck() throws SQLException {
        query("SELECT 1", rs -> {});
    }

    @Override
    public synchronized boolean isValid() throws SQLException {
        try {
            executeHealthCheck();
            return true;
        } catch (SQLException e) {
            LOG.debug("GaussDB connection is not valid", e);
            return false;
        }
    }

    private static class RetryingConnectionFactory implements ConnectionFactory {
        private final ConnectionFactory delegateFactory;

        private RetryingConnectionFactory(ConnectionFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        @Override
        public Connection connect(JdbcConfiguration config) throws SQLException {
            final int maxRetries = Math.max(1, config.getInteger(CONNECT_MAX_RETRIES_KEY, 3));
            final long retryDelayMs =
                    Math.max(0L, config.getLong(CONNECT_RETRY_DELAY_MS_KEY, 5_000L));
            final Duration timeout =
                    config.getConnectionTimeout() != null
                            ? config.getConnectionTimeout()
                            : DEFAULT_CONNECTION_TIMEOUT;

            final int previousLoginTimeoutSeconds = DriverManager.getLoginTimeout();
            final int loginTimeoutSeconds =
                    (int) Math.min(Integer.MAX_VALUE, Math.max(0L, timeout.toSeconds()));

            SQLException lastException = null;
            try {
                DriverManager.setLoginTimeout(loginTimeoutSeconds);
                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        Connection connection = delegateFactory.connect(sanitized(config));
                        if (attempt > 1) {
                            LOG.info(
                                    "Connected to GaussDB after {} attempts (timeout={}s)",
                                    attempt,
                                    loginTimeoutSeconds);
                        } else {
                            LOG.debug("Connected to GaussDB (timeout={}s)", loginTimeoutSeconds);
                        }
                        return connection;
                    } catch (SQLException e) {
                        lastException = e;
                        if (attempt >= maxRetries) {
                            LOG.error("Failed to connect to GaussDB after {} attempts", attempt, e);
                            throw e;
                        }

                        LOG.warn(
                                "Failed to connect to GaussDB (attempt {}/{}), retrying in {} ms",
                                attempt,
                                maxRetries,
                                retryDelayMs,
                                e);

                        try {
                            Thread.sleep(retryDelayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            SQLException interrupted =
                                    new SQLException(
                                            "Interrupted while retrying to connect to GaussDB", ie);
                            interrupted.addSuppressed(e);
                            throw interrupted;
                        }
                    }
                }
            } finally {
                DriverManager.setLoginTimeout(previousLoginTimeoutSeconds);
            }

            throw lastException != null
                    ? lastException
                    : new SQLException("Failed to connect to GaussDB");
        }

        private static JdbcConfiguration sanitized(JdbcConfiguration config) {
            return JdbcConfiguration.adapt(
                    config.filter(
                            key ->
                                    !CONNECT_MAX_RETRIES_KEY.equals(key)
                                            && !CONNECT_RETRY_DELAY_MS_KEY.equals(key)));
        }
    }
}
