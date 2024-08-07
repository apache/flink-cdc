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

package org.apache.flink.cdc.connectors.sqlserver.source.dialect;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch.SqlServerScanFetchTask;
import org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch.SqlServerSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch.SqlServerStreamFetchTask;
import org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.createSqlServerConnection;
import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerUtils.currentLsn;

/** The {@link JdbcDataSourceDialect} implementation for SqlServer datasource. */
@Experimental
public class SqlServerDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final SqlServerSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    private transient SqlServerSchema sqlserverSchema;

    public SqlServerDialect(SqlServerSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "SqlServer";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentLsn((SqlServerConnection) jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the redoLog offset error", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return createSqlServerConnection(sourceConfig.getDbzConnectorConfig());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new SqlServerChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new SqlServerChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new SqlServerPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        SqlServerSourceConfig sqlserverSourceConfig = (SqlServerSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return SqlServerConnectionUtils.listTables(
                    jdbcConnection,
                    sqlserverSourceConfig.getTableFilters(),
                    sqlserverSourceConfig.getDatabaseList());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (SqlServerConnection jdbc =
                createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
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
        if (sqlserverSchema == null) {
            sqlserverSchema = new SqlServerSchema();
        }
        return sqlserverSchema.getTableSchema(
                jdbc,
                tableId,
                sourceConfig.getDbzConnectorConfig().getTableFilters().dataCollectionFilter());
    }

    @Override
    public SqlServerSourceFetchTaskContext createFetchTaskContext(
            JdbcSourceConfig taskSourceConfig) {
        final SqlServerConnection jdbcConnection =
                createSqlServerConnection(sourceConfig.getDbzConnectorConfig());
        final SqlServerConnection metaDataConnection =
                createSqlServerConnection(sourceConfig.getDbzConnectorConfig());
        return new SqlServerSourceFetchTaskContext(
                taskSourceConfig, this, jdbcConnection, metaDataConnection);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new SqlServerScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new SqlServerStreamFetchTask(sourceSplitBase.asStreamSplit());
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
