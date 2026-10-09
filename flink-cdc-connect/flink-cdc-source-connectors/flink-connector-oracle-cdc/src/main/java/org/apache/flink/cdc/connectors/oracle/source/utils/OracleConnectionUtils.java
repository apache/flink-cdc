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

package org.apache.flink.cdc.connectors.oracle.source.utils;

import org.apache.flink.cdc.connectors.oracle.connection.OracleSourceConnection;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/** Oracle connection Utilities. */
public class OracleConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectionUtils.class);

    /** Returned by column metadata in Oracle if no scale is set. */
    private static final int ORACLE_UNSET_SCALE = -127;

    private static final int ORA_TIMESTAMP_TO_SCN_OUT_OF_RANGE = 8181;
    private static final int ORA_TIMESTAMP_TO_SCN_INVALID_TIMESTAMP = 8186;

    /** show current scn sql in oracle. */
    private static final String SHOW_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";

    private static final String SHOW_SCN_BY_TIMESTAMP = "SELECT timestamp_to_scn(?) FROM DUAL";
    private static final String SHOW_OLDEST_AVAILABLE_SCN =
            "SELECT MIN(FIRST_CHANGE#) FROM ("
                    + "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG "
                    + "UNION "
                    + "SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$ARCHIVED_LOG WHERE STATUS='A'"
                    + ")";

    /** Creates a new {@link OracleSourceConnection}, but not open the connection. */
    public static OracleSourceConnection createOracleConnection(Configuration configuration) {
        return createOracleConnection(JdbcConfiguration.adapt(configuration));
    }

    /** Creates a new {@link OracleSourceConnection}, but not open the connection. */
    public static OracleSourceConnection createOracleConnection(
            JdbcConfiguration dbzConfiguration) {
        Configuration configuration = dbzConfiguration.subset(DATABASE_CONFIG_PREFIX, true);
        return new OracleSourceConnection(
                configuration.isEmpty() ? dbzConfiguration : JdbcConfiguration.adapt(configuration),
                OracleConnectionUtils.class::getClassLoader);
    }

    /** Fetch current redoLog offsets in Oracle Server. */
    public static RedoLogOffset currentRedoLogOffset(JdbcConnection jdbc) {
        try {
            return jdbc.queryAndMap(
                    SHOW_CURRENT_SCN,
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            return new RedoLogOffset(Scn.valueOf(scn).longValue());
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the scn via '"
                                            + SHOW_CURRENT_SCN
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the redo log position via '"
                            + SHOW_CURRENT_SCN
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    @FunctionalInterface
    interface ScnSupplier {
        Scn get() throws SQLException;
    }

    /** Resolve the startup timestamp to a readable redo-log offset. */
    public static RedoLogOffset resolveRedoLogOffsetByTimestamp(
            OracleConnection connection, long startupTimestampMillis) {
        return resolveRedoLogOffsetByTimestamp(
                startupTimestampMillis,
                () ->
                        connection.prepareQueryAndMap(
                                SHOW_SCN_BY_TIMESTAMP,
                                statement ->
                                        statement.setTimestamp(
                                                1,
                                                Timestamp.from(
                                                        Instant.ofEpochMilli(
                                                                startupTimestampMillis))),
                                rs -> {
                                    if (rs.next()) {
                                        final String value = rs.getString(1);
                                        return value == null ? null : Scn.valueOf(value);
                                    }
                                    return null;
                                }),
                () ->
                        connection.queryAndMap(
                                SHOW_OLDEST_AVAILABLE_SCN,
                                rs -> {
                                    if (rs.next()) {
                                        final String value = rs.getString(1);
                                        return value == null ? Scn.NULL : Scn.valueOf(value);
                                    }
                                    return Scn.NULL;
                                }));
    }

    static RedoLogOffset resolveRedoLogOffsetByTimestamp(
            long startupTimestampMillis,
            ScnSupplier timestampScnSupplier,
            ScnSupplier oldestScnSupplier) {
        try {
            final Scn timestampScn = timestampScnSupplier.get();

            if (timestampScn != null && !timestampScn.isNull()) {
                LOG.info(
                        "Resolved startup timestamp {} to Oracle SCN {}.",
                        startupTimestampMillis,
                        timestampScn);
                return new RedoLogOffset(timestampScn.subtract(Scn.ONE).longValue());
            }
        } catch (SQLException e) {
            if (isTimestampToScnFallbackError(e)) {
                LOG.warn(
                        "Cannot resolve startup timestamp {} to SCN due to {}, fallback to oldest available SCN.",
                        startupTimestampMillis,
                        e.getMessage());
            } else {
                throw new FlinkRuntimeException(
                        "Cannot resolve startup timestamp "
                                + startupTimestampMillis
                                + " to Oracle SCN.",
                        e);
            }
        }

        try {
            final Scn oldestScn = oldestScnSupplier.get();
            if (oldestScn == null || oldestScn.isNull()) {
                throw new FlinkRuntimeException(
                        "Cannot resolve oldest available Oracle SCN for startup timestamp "
                                + startupTimestampMillis);
            }
            LOG.warn(
                    "Fallback startup timestamp {} to oldest available Oracle SCN {}.",
                    startupTimestampMillis,
                    oldestScn);
            return new RedoLogOffset(oldestScn.subtract(Scn.ONE).longValue());
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot query oldest available Oracle SCN for startup timestamp "
                            + startupTimestampMillis,
                    e);
        }
    }

    private static boolean isTimestampToScnFallbackError(SQLException e) {
        if (e.getErrorCode() == ORA_TIMESTAMP_TO_SCN_OUT_OF_RANGE
                || e.getErrorCode() == ORA_TIMESTAMP_TO_SCN_INVALID_TIMESTAMP) {
            return true;
        }
        String message = e.getMessage();
        return message != null
                && (message.startsWith("ORA-08181") || message.startsWith("ORA-08186"));
    }

    public static List<TableId> listTables(
            JdbcConnection jdbcConnection, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX') AND NESTED = 'NO' AND TABLE_NAME NOT IN (SELECT PARENT_TABLE_NAME FROM ALL_NESTED_TABLES)";
        try {
            jdbcConnection.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId =
                                    new TableId(jdbcConnection.database(), schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                LOG.info("\t including '{}' for further processing", tableId);
            } else {
                LOG.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }
}
