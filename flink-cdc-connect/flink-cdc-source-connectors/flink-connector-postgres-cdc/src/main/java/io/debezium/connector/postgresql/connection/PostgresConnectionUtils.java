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

public class PostgresConnectionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

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

        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, lsn.toString());
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, txId.toString());
        }
        offsetMap.put(
                SourceInfo.TIMESTAMP_USEC_KEY,
                String.valueOf(Conversions.toEpochMicros(Instant.MIN)));
        return PostgresOffset.of(offsetMap);
    }
}
