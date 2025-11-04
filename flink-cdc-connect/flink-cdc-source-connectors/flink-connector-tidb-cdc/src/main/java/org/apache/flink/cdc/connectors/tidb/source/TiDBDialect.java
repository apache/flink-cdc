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

package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnectionPoolFactory;
import org.apache.flink.cdc.connectors.tidb.source.fetch.TiDBScanFetchTask;
import org.apache.flink.cdc.connectors.tidb.source.fetch.TiDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.tidb.source.fetch.TiDBStreamFetchTask;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBSchema;
import org.apache.flink.cdc.connectors.tidb.source.splitter.TiDBChunkSplitter;
import org.apache.flink.cdc.connectors.tidb.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBConnectionUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** TiDB data source dialect. */
public class TiDBDialect implements JdbcDataSourceDialect {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBDialect.class);

    private static final String QUOTED_CHARACTER = "`";
    private static final long serialVersionUID = 1L;

    private final TiDBSourceConfig sourceConfig;
    private transient TiDBSchema tiDBSchema;
    @Nullable private TiDBStreamFetchTask streamFetchTask;

    public TiDBDialect(TiDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "TiDB";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TiDBUtils.currentBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
        //    return null;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TiDBConnectionUtils.isTableIdCaseInsensitive(jdbcConnection);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading TiDB variables: " + e.getMessage(), e);
        }
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new TiDBChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new TiDBChunkSplitter(this.sourceConfig, this, chunkSplitterState);
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        return new TiDBSourceFetchTaskContext(sourceConfig, this, openJdbcConnection());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        // temp
        return true;
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            List<TableId> tableIds =
                    TableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters());
            if (tableIds.isEmpty()) {
                throw new FlinkRuntimeException(
                        "No tables discovered for the given tables:" + sourceConfig.getTableList());
            }
            return tableIds;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables:" + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
            JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChanges.TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        TiDBSourceConfig tiDBSourceConfig = (TiDBSourceConfig) sourceConfig;
        TiDBConnectorConfig dbzConfig = tiDBSourceConfig.getDbzConnectorConfig();

        JdbcConnection jdbc =
                new TiDBConnection(
                        dbzConfig.getJdbcConfig(),
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                        QUOTED_CHARACTER,
                        QUOTED_CHARACTER);
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open TiDB connection", e);
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    public TiDBConnection openJdbcConnection() {
        return (TiDBConnection) openJdbcConnection(sourceConfig);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new TiDBConnectionPoolFactory();
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (tiDBSchema == null) {
            tiDBSchema =
                    new TiDBSchema(sourceConfig, isDataCollectionIdCaseSensitive(sourceConfig));
        }
        return tiDBSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new TiDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new TiDBStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public void close() throws IOException {
        JdbcDataSourceDialect.super.close();
    }
}
