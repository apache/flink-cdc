package org.apache.flink.cdc.connectors.tidb.source.config;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors;
import io.debezium.relational.Tables;

public class TiDBConnectorConfig extends RelationalDatabaseConnectorConfig {
    protected static final String LOGICAL_NAME = "tidb_cdc_connector";

    public TiDBConnectorConfig(
            Configuration config,
            String logicalName,
            Tables.TableFilter systemTablesFilter,
            Selectors.TableIdToStringMapper tableIdMapper,
            int defaultSnapshotFetchSize,
            ColumnFilterMode columnFilterMode) {
        super(
                config,
                logicalName,
                systemTablesFilter,
                tableIdMapper,
                defaultSnapshotFetchSize,
                columnFilterMode);
    }

    public TiDBConnectorConfig(TiDBSourceConfig tiDBSourceConfig) {
        this(null, null, null, null, 0, null);
    }

    @Override
    public String getContextName() {
        return "TiDB";
    }

    @Override
    public String getConnectorName() {
        return "TiDB";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return null;
    }
}
