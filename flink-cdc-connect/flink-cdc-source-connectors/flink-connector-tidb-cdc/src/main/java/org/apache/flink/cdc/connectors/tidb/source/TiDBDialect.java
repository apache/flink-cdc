package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBSchema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;

public class TiDBDialect implements JdbcDataSourceDialect {
    private static final long serialVersionUID = 1L;

    private transient TiDBSchema tiDBSchema;

    @Override
    public String getName() {
        return "TiDB";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        return false;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        return false;
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
            JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return null;
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        return null;
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        return null;
    }
}
