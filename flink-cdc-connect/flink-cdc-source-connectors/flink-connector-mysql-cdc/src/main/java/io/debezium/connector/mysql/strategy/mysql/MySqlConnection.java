/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.strategy.mysql;

import com.mysql.cj.CharsetMapping;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlFieldReader;
import io.debezium.connector.mysql.MySqlTextProtocolFieldReader;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.function.Predicate;

/**
 * Copied from Debezium project(2.5.4.Final) to add MySQL 8.4+ compatibility.
 *
 * <p>MySQL 8.4 removed {@code SHOW MASTER STATUS} in favour of {@code SHOW BINARY LOG STATUS}. The
 * statement to use is probed once, on construction, and exposed through {@link
 * #getShowBinaryLogStatement()} so the snapshot and streaming sources can use the same one.
 *
 * <p>Added {@link #MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT}, {@link
 * #MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT}, the {@link #showBinaryLogStatement} field, {@link
 * #probeShowBinaryLogStatement()} and {@link #getShowBinaryLogStatement()}; {@link #knownGtidSet()}
 * uses the probed statement instead of a hard-coded {@code SHOW MASTER STATUS}.
 *
 * <p>Also added the single-argument constructor used by Flink CDC's own connection factory.
 *
 * <p>Overrode {@link #connectionString()} so it reports the url actually used to connect. Debezium
 * builds it from the fixed {@code AbstractConnectionConfiguration.URL_PATTERN}, which ignores the
 * pattern {@link MySqlConnectionConfiguration} builds from the user's {@code jdbc.properties.*}.
 */
public class MySqlConnection extends AbstractConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    private static final String MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT = "SHOW MASTER STATUS";
    private static final String MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT = "SHOW BINARY LOG STATUS";

    private final String showBinaryLogStatement;

    public MySqlConnection(MySqlConnectionConfiguration connectionConfig) {
        this(connectionConfig, new MySqlTextProtocolFieldReader(null));
    }

    public MySqlConnection(
            MySqlConnectionConfiguration connectionConfig, MySqlFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
        this.showBinaryLogStatement = probeShowBinaryLogStatement();
    }

    @Override
    public String connectionString() {
        return connectionString(
                ((MySqlConnectionConfiguration) connectionConfig()).getUrlPattern());
    }

    @Override
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return "ON".equalsIgnoreCase(rs.getString(2));
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet knownGtidSet() {
        try {
            return queryAndMap(
                    showBinaryLogStatement,
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                            // GTID set, may be null, blank, or contain a GTID set
                            return new MySqlGtidSet(rs.getString(5));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap(
                    "SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while executing GTID_SUBTRACT: ", e);
        }
    }

    @Override
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap(
                    "SELECT @@global.gtid_purged",
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                            // GTID set, may be null, blank, or contain a GTID set
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Unexpected error while looking at gtid_purged variable: ", e);
        }
    }

    @Override
    public GtidSet filterGtidSet(
            Predicate<String> gtidSourceFilter,
            String offsetGtids,
            GtidSet availableServerGtidSet,
            GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new MySqlGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info(
                    "GTID set after applying GTID source includes/excludes to previous recorded offset: {}",
                    filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        GtidSet mergedGtidSet;

        // Debezium 2.0 removed the gtid.new.channel.position config enum; Flink CDC keeps the
        // behaviour behind the pass-through property of the same name. It defaults to "earliest",
        // matching upstream Debezium 2.0.
        String newChannelPosition =
                connectionConfig().originalConfig().getString("gtid.new.channel.position");
        boolean useLatest = "latest".equalsIgnoreCase(newChannelPosition);

        if (!useLatest) {
            final GtidSet knownGtidSet = filteredGtidSet;
            LOGGER.info("Using first available positions for new GTID channels");
            final GtidSet relevantAvailableServerGtidSet =
                    (gtidSourceFilter != null)
                            ? availableServerGtidSet.retainAll(gtidSourceFilter)
                            : availableServerGtidSet;
            LOGGER.info(
                    "Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

            mergedGtidSet =
                    GtidUtils.fixOldChannelsGtidSet(
                            (MySqlGtidSet) relevantAvailableServerGtidSet,
                            (MySqlGtidSet) purgedServerGtidSet,
                            (MySqlGtidSet) knownGtidSet);
        } else {
            LOGGER.info("Using latest positions for new GTID channels");
            mergedGtidSet =
                    GtidUtils.computeLatestModeGtidSet(
                            (MySqlGtidSet) availableServerGtidSet,
                            (MySqlGtidSet) purgedServerGtidSet,
                            (MySqlGtidSet) filteredGtidSet,
                            gtidSourceFilter);
        }

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    @Override
    public boolean isMariaDb() {
        return false;
    }

    @Override
    protected GtidSet createGtidSet(String gtids) {
        return new MySqlGtidSet(gtids);
    }

    /**
     * Returns the statement this server understands for reading the current binary log coordinates.
     *
     * <p>MySQL 8.4 removed {@code SHOW MASTER STATUS}; {@code SHOW BINARY LOG STATUS} replaces it.
     */
    public String getShowBinaryLogStatement() {
        return showBinaryLogStatement;
    }

    private String probeShowBinaryLogStatement() {
        LOGGER.info("Probing binary log statement.");
        try {
            // Attempt to query
            query(MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT, rs -> {});
            LOGGER.info(
                    "Successfully found show binary log statement with `{}`.",
                    MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT);
            return MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT;
        } catch (SQLException e) {
            LOGGER.info(
                    "Probing with {} failed, fallback to classic {}. Caused by: {}",
                    MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT,
                    MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT,
                    e.getMessage());
            return MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT;
        }
    }

    public static String getJavaEncodingForCharSet(String charSetName) {
        return CharsetMappingWrapper.getJavaEncodingForMysqlCharSet(charSetName);
    }

    /** Helper to gain access to protected method. */
    private static final class CharsetMappingWrapper extends CharsetMapping {
        static String getJavaEncodingForMysqlCharSet(String charSetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(charSetName);
        }
    }
}
