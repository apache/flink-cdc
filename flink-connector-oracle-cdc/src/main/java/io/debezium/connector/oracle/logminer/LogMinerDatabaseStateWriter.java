/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that can write the contents of several Oracle LogMiner tables to the connector's
 * log if and only if DEBUG logging is enabled.
 */
public class LogMinerDatabaseStateWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerDatabaseStateWriter.class);

    public static void write(OracleConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Configured redo logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGFILE");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain redo log table entries", e);
            }
            LOGGER.debug("Archive logs for last 48 hours:");
            try {
                logQueryResults(
                        connection, "SELECT * FROM V$ARCHIVED_LOG WHERE FIRST_TIME >= SYSDATE - 2");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain archive log table entries", e);
            }
            LOGGER.debug("Available logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOG");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log table entries", e);
            }
            LOGGER.debug("Log history last 24 hours:");
            try {
                logQueryResults(
                        connection, "SELECT * FROM V$LOG_HISTORY WHERE FIRST_TIME >= SYSDATE - 1");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log history", e);
            }
            LOGGER.debug("Log entries registered with LogMiner are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_LOGS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain registered logs with LogMiner", e);
            }
            LOGGER.debug("Log mining session parameters are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_PARAMETERS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log mining session parameters", e);
            }
        }
    }

    /**
     * Helper method that dumps the result set of an arbitrary SQL query to the connector's logs.
     *
     * @param connection the database connection
     * @param query the query to execute
     * @throws SQLException thrown if an exception occurs performing a SQL operation
     */
    private static void logQueryResults(OracleConnection connection, String query)
            throws SQLException {
        connection.query(
                query,
                rs -> {
                    int columns = rs.getMetaData().getColumnCount();
                    List<String> columnNames = new ArrayList<>();
                    for (int index = 1; index <= columns; ++index) {
                        columnNames.add(rs.getMetaData().getColumnName(index));
                    }
                    LOGGER.debug("{}", columnNames);
                    while (rs.next()) {
                        List<Object> columnValues = new ArrayList<>();
                        for (int index = 1; index <= columns; ++index) {
                            columnValues.add(rs.getObject(index));
                        }
                        LOGGER.debug("{}", columnValues);
                    }
                });
    }
}
