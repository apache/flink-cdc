package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TiDBSourceConfig extends JdbcSourceConfig {
    private static final long serialVersionUID = 1L;
    private final String compatibleMode;
    private final String pdAddresses;

    private final String hostMapping;

    public TiDBSourceConfig(
            String compatibleMode,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> tableList,
            String pdAddresses,
            String hostMapping,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled) {
        super(
                startupOptions,
                databaseList,
                null,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled);
        this.compatibleMode = compatibleMode;
        this.pdAddresses = pdAddresses;
        this.hostMapping = hostMapping;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public String getPdAddresses() {
        return pdAddresses;
    }

    public String getHostMapping() {
        return hostMapping;
    }

    @Override
    public TiDBConnectorConfig getDbzConnectorConfig() {
        return new TiDBConnectorConfig(this);
    }
}
