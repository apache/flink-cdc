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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.oracle.source.assigner.splitter.OracleChunkSplitter;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleStreamFetchTask;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleSchema;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;
import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils.currentRedoLogOffset;

/** The {@link JdbcDataSourceDialect} implementation for Oracle datasource. */
@Experimental
public class OracleDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private transient OracleSchema oracleSchema;

    private transient Tables.TableFilter filters;

    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentRedoLogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the redoLog offset error", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            OracleConnection oracleConnection = (OracleConnection) jdbcConnection;
            return oracleConnection.getOracleVersion().getMajor() == 11;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading oracle variables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return OracleConnectionUtils.createOracleConnection(
                sourceConfig.getDbzConnectorConfig().getJdbcConfig());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new OracleChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new OracleChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new OraclePooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        OracleSourceConfig oracleSourceConfig = (OracleSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return OracleConnectionUtils.listTables(
                    jdbcConnection, oracleSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (OracleConnection jdbc = createOracleConnection(sourceConfig.getDbzConfiguration())) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (oracleSchema == null) {
            oracleSchema = new OracleSchema();
        }
        return oracleSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public OracleSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new OracleSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OracleScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new OracleStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }

        return filters.isIncluded(tableId);
    }
}
