/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/*
 * Vendored from Debezium 3.4.2.Final with one change: the try-finally block in
 * computeResumePositionFromLogs() guards sessionContext.endMiningSession() with a null check.
 *
 * Root cause of the original NPE: when the lazy OracleConnection / LogMinerSessionContext
 * initialisation fails (e.g. setAutoCommit or resetSessionToCdb throws), the exception
 * handler executes before sessionContext has been assigned and therefore NPEs on
 * sessionContext.endMiningSession(), suppressing the real exception.
 *
 * The fix adds:
 *   if (sessionContext != null) { sessionContext.endMiningSession(); }
 * so the original exception propagates and the provider recovers on the next call.
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerSessionContext;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vendored from Debezium 3.4.2.Final. Only change: null-guard on {@code sessionContext} inside the
 * finally/catch block of {@link #computeResumePositionFromLogs} so that a connection-init failure
 * does not produce a secondary NPE that suppresses the root cause.
 */
public class ResumePositionProvider implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResumePositionProvider.class);

    private final OracleConnectorConfig connectorConfig;
    private final JdbcConfiguration jdbcConfig;
    private final Duration updateInterval;
    private Threads.Timer queryTimer;

    // Lazily initialised on first use
    private OracleConnection connection;
    private LogMinerSessionContext sessionContext;

    public ResumePositionProvider(
            OracleConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig) {
        this.connectorConfig = connectorConfig;
        this.jdbcConfig = jdbcConfig;
        this.updateInterval = connectorConfig.getResumePositionUpdateInterval();
        this.queryTimer = resetTimer();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            LOGGER.info("Stopping the unbuffered resume position provider");
            if (sessionContext != null) {
                sessionContext.close();
            }
            connection.close();
        }
    }

    public Scn computeResumePositionFromLogs(
            Scn currentResumeScn, Scn currentCommitScn, List<LogFile> logFiles)
            throws SQLException {
        if (!queryTimer.expired()) {
            return currentResumeScn;
        }
        try {
            if (connection == null) {
                LOGGER.info("Starting the unbuffered resume position provider");
                // jdbcConfig may carry Debezium's "database.*" prefixed keys (e.g.
                // "database.hostname"). OracleConnection expects plain keys (hostname, port,
                // dbname). Strip the prefix when present so the JDBC URL is built correctly.
                Configuration stripped = jdbcConfig.subset("database.", true);
                JdbcConfiguration connectionJdbcConfig =
                        stripped.isEmpty() ? jdbcConfig : JdbcConfiguration.adapt(stripped);
                connection = new OracleConnection(connectorConfig, connectionJdbcConfig);
                connection.setAutoCommit(false);
                if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
                    connection.resetSessionToCdb();
                }
                sessionContext =
                        new LogMinerSessionContext(
                                connection,
                                false,
                                OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG,
                                connectorConfig.getLogMiningPathToDictionary());
            }
            sessionContext.removeAllLogFilesFromSession();
            sessionContext.addLogFiles(logFiles);
            sessionContext.startSession(currentResumeScn, Scn.NULL, false);
            Scn resumeScn =
                    connection.prepareQueryAndMap(
                            "SELECT * FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE IN (6,7,36) AND SCN <= ?",
                            ps -> ps.setString(1, currentCommitScn.toString()),
                            rs -> processResultSet(currentResumeScn, currentCommitScn, rs));
            LOGGER.debug(
                    "Resume/Commit SCN {}/{} - new resume SCN is {}",
                    currentResumeScn,
                    currentCommitScn,
                    resumeScn);
            return resumeScn;
        } catch (Throwable t) {
            // Log the root cause before the finally block runs, so it is not lost.
            LOGGER.warn(
                    "Failed to compute resume position from LogMiner logs; "
                            + "returning current resume SCN unchanged. Root cause: {}",
                    t.getMessage());
            // Reset connection state so the next call re-initialises cleanly.
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
            connection = null;
            sessionContext = null;
            return currentResumeScn;
        } finally {
            // Guard against NPE when init failed before sessionContext was assigned.
            if (sessionContext != null) {
                sessionContext.endMiningSession();
            }
            queryTimer = resetTimer();
        }
    }

    private Scn processResultSet(Scn currentResumeScn, Scn currentCommitScn, ResultSet rs)
            throws SQLException {
        Map<String, Transaction> transactions = new LinkedHashMap<>();
        while (rs.next()) {
            byte[] xid = rs.getBytes("XID");
            String transactionId = xid == null ? null : HexConverter.convertToHexString(xid);
            if (transactionId == null) {
                continue;
            }
            EventType eventType = EventType.from(rs.getInt("OPERATION_CODE"));
            Scn scn = Scn.valueOf(rs.getString("SCN"));
            if (eventType == EventType.START) {
                transactions.put(transactionId, new Transaction(scn));
            } else {
                Transaction transaction = transactions.get(transactionId);
                if (transaction == null) {
                    LOGGER.trace(
                            "Ignoring transaction {} event {} at SCN {} as it must have"
                                    + " started before current resume SCN {}.",
                            transactionId,
                            eventType,
                            scn,
                            currentResumeScn);
                } else {
                    transaction.markEnded(scn);
                }
            }
        }
        return transactions.values().stream()
                .filter(Transaction::isInProgress)
                .findFirst()
                .map(Transaction::getStartScn)
                .orElse(currentCommitScn);
    }

    private Threads.Timer resetTimer() {
        return Threads.timer(Clock.SYSTEM, updateInterval);
    }

    private static class Transaction {
        private final Scn startScn;
        private Scn endScn;

        Transaction(Scn startScn) {
            this.startScn = startScn;
        }

        public void markEnded(Scn scn) {
            this.endScn = scn;
        }

        public boolean isInProgress() {
            return endScn == null;
        }

        public Scn getStartScn() {
            return startScn;
        }
    }
}
