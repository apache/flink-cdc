/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.gtid.GtidUtils;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.function.Predicate;

/**
 * Copied from Debezium project(2.7.4.Final).
 *
 * <p>Change 1: MySQL 8.4 removed {@code SHOW MASTER STATUS} in favour of {@code SHOW BINARY LOG
 * STATUS}. The statement is probed once, on construction, and exposed through {@link
 * #getShowBinaryLogStatement()} so the snapshot source can use the same one; {@link
 * #knownGtidSet()} uses it instead of a hard-coded statement.
 *
 * <p>Change 2: {@link #filterGtidSet} honours the {@code gtid.new.channel.position} pass-through
 * property (which Debezium 2.0 removed as a config enum), delegating to {@code GtidUtils} for the
 * earliest/latest new-channel reconciliation.
 *
 * <p>Change 3: add the single-argument constructor used by Flink CDC's own connection factory.
 */
public class MySqlConnection extends BinlogConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    private static final String MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT = "SHOW MASTER STATUS";
    private static final String MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT = "SHOW BINARY LOG STATUS";

    private final String showBinaryLogStatement;

    public MySqlConnection(MySqlConnectionConfiguration connectionConfig) {
        this(connectionConfig, new MySqlTextProtocolFieldReader(null));
    }

    public MySqlConnection(
            MySqlConnectionConfiguration connectionConfig, BinlogFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
        this.showBinaryLogStatement = probeShowBinaryLogStatement();
    }

    /**
     * Returns the statement this server understands for reading the current binary log coordinates.
     */
    public String getShowBinaryLogStatement() {
        return showBinaryLogStatement;
    }

    private String probeShowBinaryLogStatement() {
        LOGGER.info("Probing binary log statement.");
        try {
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
                            return new MySqlGtidSet(
                                    rs.getString(
                                            5)); // GTID set, may be null, blank, or contain a GTID
                            // set
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
                            return new MySqlGtidSet(
                                    rs.getString(
                                            1)); // GTID set, may be null, blank, or contain a GTID
                            // set
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
        final String newChannelPosition =
                connectionConfig().originalConfig().getString("gtid.new.channel.position");
        final boolean useLatest = "latest".equalsIgnoreCase(newChannelPosition);

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
}
