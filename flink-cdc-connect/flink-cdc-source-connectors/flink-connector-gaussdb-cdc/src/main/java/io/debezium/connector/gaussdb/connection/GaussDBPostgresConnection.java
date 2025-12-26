/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb.connection;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

/**
 * A {@link PostgresConnection} variant for GaussDB.
 *
 * <p>GaussDB exposes a PostgreSQL-compatible protocol, but it may report the server version in a
 * format that PostgreSQL JDBC driver cannot map to PostgreSQL major/minor versions (e.g. "gaussdb
 * (GaussDB Kernel 505.2.1 ...)"). Debezium's {@link PostgresConnection} runs a minimum server
 * version check (>= 9.4) during connection initialization; this can fail even though GaussDB is
 * compatible.
 *
 * <p>This class detects that failure and skips the minimum version validation when connecting to
 * GaussDB.
 */
public class GaussDBPostgresConnection extends PostgresConnection {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBPostgresConnection.class);

    private static final String MINIMUM_VERSION_ERROR =
            "Cannot connect to a version of Postgres lower than 9.4";

    public GaussDBPostgresConnection(JdbcConfiguration config, String connectionUsage) {
        super(config, connectionUsage);
    }

    @Override
    public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
        try {
            return super.connection(executeOnConnect);
        } catch (SQLException versionValidationFailure) {
            if (!isMinimumVersionValidationFailure(versionValidationFailure)) {
                throw versionValidationFailure;
            }

            // At this point, the underlying JDBC connection is typically already established and
            // cached by JdbcConnection; the failure is from Debezium's initial operations.
            final Connection establishedConnection;
            try {
                establishedConnection = super.connection(false);
            } catch (SQLException followUpFailure) {
                versionValidationFailure.addSuppressed(followUpFailure);
                throw versionValidationFailure;
            }

            if (!looksLikeGaussDb(establishedConnection)) {
                throw versionValidationFailure;
            }

            LOG.info(
                    "Skipping Debezium PostgreSQL minimum version validation for GaussDB; server reports: {}",
                    safeSelectVersion(establishedConnection));

            // Preserve JdbcConnection behavior for optional on-connect statements.
            if (executeOnConnect) {
                final String statements =
                        config().getString(JdbcConfiguration.ON_CONNECT_STATEMENTS);
                if (statements != null) {
                    final List<String> splitStatements = parseSqlStatementString(statements);
                    super.execute(splitStatements.toArray(new String[0]));
                }
            }

            return establishedConnection;
        }
    }

    private static boolean isMinimumVersionValidationFailure(SQLException failure) {
        final String message = failure.getMessage();
        return message != null && message.contains(MINIMUM_VERSION_ERROR);
    }

    private static boolean looksLikeGaussDb(Connection connection) {
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            if (containsIgnoreCase(metaData.getDatabaseProductName(), "gaussdb")
                    || containsIgnoreCase(metaData.getDatabaseProductVersion(), "gaussdb")) {
                return true;
            }
        } catch (SQLException ignored) {
            // ignore
        }
        final String version = safeSelectVersion(connection);
        return containsIgnoreCase(version, "gaussdb");
    }

    private static String safeSelectVersion(Connection connection) {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT version()")) {
            return rs.next() ? rs.getString(1) : "<unknown>";
        } catch (SQLException e) {
            LOG.debug("Failed to query GaussDB version()", e);
            return "<unknown>";
        }
    }

    private static boolean containsIgnoreCase(String value, String needle) {
        if (value == null || needle == null) {
            return false;
        }
        return value.toLowerCase(Locale.ROOT).contains(needle.toLowerCase(Locale.ROOT));
    }
}
