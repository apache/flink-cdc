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

package org.apache.flink.cdc.connectors.sqlserver.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;
import org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerUtils;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.connector.sqlserver.SqlServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/** A factory to create {@link LsnOffset}. */
public class LsnFactory extends OffsetFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LsnFactory.class);
    private static final String MAP_TIME_TO_LSN_QUERY =
            "SELECT %s.sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', ?)";
    private static final String MAP_LSN_TO_TIME_QUERY = "SELECT %s.sys.fn_cdc_map_lsn_to_time(?)";

    private final SqlServerSourceConfig sourceConfig;

    public LsnFactory() {
        this(null);
    }

    public LsnFactory(SqlServerSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Offset newOffset(Map<String, String> offset) {
        Lsn changeLsn = Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
        Lsn commitLsn = Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
        Long eventSerialNo = null;
        if (offset.get(SourceInfo.EVENT_SERIAL_NO_KEY) != null) {
            eventSerialNo = Long.valueOf(offset.get(SourceInfo.EVENT_SERIAL_NO_KEY));
        }
        return new LsnOffset(changeLsn, commitLsn, eventSerialNo);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        if (sourceConfig == null) {
            throw new UnsupportedOperationException(
                    "Timestamp startup mode requires SqlServerSourceConfig in LsnFactory.");
        }
        List<String> databaseList = sourceConfig.getDatabaseList();
        if (databaseList == null || databaseList.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot create timestamp offset without configured SQL Server database.");
        }
        final String databaseName = databaseList.get(0);
        final ZoneId serverZoneId = ZoneId.of(sourceConfig.getServerTimeZone());
        final Timestamp startupTimestamp =
                Timestamp.valueOf(
                        Instant.ofEpochMilli(timestampMillis)
                                .atZone(serverZoneId)
                                .toLocalDateTime());
        final String mapTimeToLsnQuery =
                String.format(MAP_TIME_TO_LSN_QUERY, SqlServerUtils.quote(databaseName));
        final Calendar serverCalendar = Calendar.getInstance(TimeZone.getTimeZone(serverZoneId));
        try (SqlServerConnection connection =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            Lsn mappedLsn =
                    connection.prepareQueryAndMap(
                            mapTimeToLsnQuery,
                            statement ->
                                    statement.setTimestamp(1, startupTimestamp, serverCalendar),
                            rs -> {
                                if (!rs.next()) {
                                    throw new SQLException(
                                            "No result returned when mapping timestamp to LSN.");
                                }
                                return Lsn.valueOf(rs.getBytes(1));
                            });
            if (!mappedLsn.isAvailable()) {
                Lsn latestLsn = connection.getMaxTransactionLsn(databaseName);
                if (!latestLsn.isAvailable()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Cannot create timestamp offset for %s because SQL Server returns no available LSN.",
                                    databaseName));
                }
                LOG.warn(
                        "No LSN is mapped for startup timestamp {} on database {}, fallback to latest LSN {}.",
                        timestampMillis,
                        databaseName,
                        latestLsn);
                mappedLsn = latestLsn;
            }
            logResolvedStartupLsn(
                    connection,
                    databaseName,
                    timestampMillis,
                    startupTimestamp,
                    serverZoneId,
                    mappedLsn);
            return new LsnOffset(mappedLsn, mappedLsn, null);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to create SQL Server offset from timestamp %s for database %s.",
                            timestampMillis, databaseName),
                    e);
        }
    }

    @Override
    public Offset createInitialOffset() {
        return LsnOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return LsnOffset.NO_STOPPING_OFFSET;
    }

    private void logResolvedStartupLsn(
            SqlServerConnection connection,
            String databaseName,
            long timestampMillis,
            Timestamp startupTimestamp,
            ZoneId serverZoneId,
            Lsn restartLsn) {
        try {
            Timestamp restartTimestamp =
                    connection.prepareQueryAndMap(
                            String.format(
                                    MAP_LSN_TO_TIME_QUERY, SqlServerUtils.quote(databaseName)),
                            statement -> statement.setBytes(1, restartLsn.getBinary()),
                            rs -> {
                                if (!rs.next()) {
                                    throw new SQLException(
                                            "No result returned when mapping LSN to timestamp.");
                                }
                                return rs.getTimestamp(1);
                            });
            LOG.info(
                    "Resolved SQL Server startup timestamp {} (server time: {}) to restart LSN {} with commit time {} on database {}.",
                    timestampMillis,
                    startupTimestamp.toLocalDateTime().atZone(serverZoneId),
                    restartLsn,
                    restartTimestamp == null
                            ? "unknown"
                            : restartTimestamp.toLocalDateTime().atZone(serverZoneId),
                    databaseName);
        } catch (Exception e) {
            LOG.info(
                    "Resolved SQL Server startup timestamp {} (server time: {}) to restart LSN {} on database {}.",
                    timestampMillis,
                    startupTimestamp.toLocalDateTime().atZone(serverZoneId),
                    restartLsn,
                    databaseName);
            LOG.warn(
                    "Failed to query commit time for restart LSN {} on database {}, but startup continues.",
                    restartLsn,
                    databaseName,
                    e);
        }
    }
}
