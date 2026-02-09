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

package io.debezium.connector.postgresql.connection;

import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.Utils;
import io.debezium.time.Conversions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class for accessing various Debezium PostgresConnection private-package classes/methods
 */
public class PostgresConnectionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * PostgreSQL 11 introduced declarative partitioning with improved partition table handling. In
     * PostgreSQL 10 and earlier, replication cannot be created on partition parent tables, so these
     * tables must be excluded when determining captured tables for CDC.
     */
    private static final int PG_DECLARATIVE_PARTITION_MIN_VERSION = 11;

    public static PostgresOffset committedOffset(
            PostgresConnection jdbcConnection, String slotName, String pluginName) {
        Long lsn;
        Long txId;
        try {
            ServerInfo.ReplicationSlot slot =
                    jdbcConnection.readReplicationSlotInfo(slotName, pluginName);

            if (slot == ServerInfo.ReplicationSlot.INVALID) {
                return Utils.currentOffset(jdbcConnection);
            }
            lsn = slot.latestFlushedLsn().asLong();
            txId = slot.catalogXmin();
            LOGGER.trace("Read xlogStart at '{}' from transaction '{}'", Lsn.valueOf(lsn), txId);
        } catch (SQLException | InterruptedException e) {
            throw new FlinkRuntimeException("Error getting current Lsn/txId " + e.getMessage(), e);
        }

        try {
            jdbcConnection.commit();
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "JDBC connection fails to commit: " + e.getMessage(), e);
        }

        Map<String, String> offsetMap = new HashMap<>(8);
        offsetMap.put(SourceInfo.LSN_KEY, lsn.toString());
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, txId.toString());
        }
        offsetMap.put(
                SourceInfo.TIMESTAMP_USEC_KEY,
                String.valueOf(Conversions.toEpochMicros(Instant.MIN)));
        return PostgresOffset.of(offsetMap);
    }

    /**
     * Checks if partition parent tables should be excluded from CDC replication. In PostgreSQL 10
     * and earlier versions, logical replication (publications) cannot be created on partition
     * parent tables, only on the actual partition child tables. Starting from PostgreSQL 11,
     * declarative partitioning was improved to allow replication on parent tables.
     *
     * @param jdbcConnection the PostgresConnection instance
     * @return true if partitioned parent tables should be excluded (for PG 10 and earlier), false
     *     otherwise (for PG 11+)
     * @throws SQLException if unable to retrieve server information
     */
    public static boolean shouldExcludePartitionTables(PostgresConnection jdbcConnection)
            throws SQLException {
        int majorVersion = jdbcConnection.connection().getMetaData().getDatabaseMajorVersion();
        return majorVersion < PG_DECLARATIVE_PARTITION_MIN_VERSION;
    }
}
