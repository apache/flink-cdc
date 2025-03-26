package io.debezium.connector.tidb;

import io.debezium.pipeline.spi.Partition;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class TiDBPartition implements Partition {
    private final String serverName;

    public TiDBPartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getLoggingContext() {
        return Partition.super.getLoggingContext();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final io.debezium.connector.tidb.TiDBPartition other =
                (io.debezium.connector.tidb.TiDBPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
