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

package org.apache.flink.cdc.connectors.oceanbase.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseSourceConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnectionPoolFactory;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.LogMessageOffset;
import org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch.OceanBaseScanFetchTask;
import org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch.OceanBaseSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch.OceanBaseStreamFetchTask;
import org.apache.flink.cdc.connectors.oceanbase.source.schema.OceanBaseSchema;
import org.apache.flink.cdc.connectors.oceanbase.source.splitter.OceanBaseChunkSplitter;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The {@link JdbcDataSourceDialect} implementation for OceanBase datasource. */
public class OceanBaseDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;

    private transient OceanBaseSchema obSchema;

    @Override
    public String getName() {
        return "OceanBase";
    }

    @Override
    public OceanBaseConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        String compatibleMode = ((OceanBaseSourceConfig) sourceConfig).getCompatibleMode();
        String quote = "mysql".equalsIgnoreCase(compatibleMode) ? "`" : "\"";
        OceanBaseConnection jdbcConnection =
                new OceanBaseConnection(
                        sourceConfig.getDbzConnectorConfig().getJdbcConfig(),
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                        compatibleMode,
                        quote,
                        quote);
        try {
            jdbcConnection.connect();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return jdbcConnection;
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (OceanBaseConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return LogMessageOffset.from(jdbcConnection.getCurrentTimestampS());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to query current timestamp", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (OceanBaseConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return !"0".equals(jdbcConnection.readSystemVariable("lower_case_table_names"));
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to determine case sensitivity", e);
        }
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new OceanBaseChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return createChunkSplitter(sourceConfig);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (OceanBaseConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return jdbcConnection.listTables(sourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
            JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChanges.TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to discover table schemas", e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new OceanBaseConnectionPoolFactory();
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (obSchema == null) {
            obSchema = new OceanBaseSchema();
        }
        return obSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OceanBaseScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new OceanBaseStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        return new OceanBaseSourceFetchTaskContext(
                sourceConfig, this, openJdbcConnection(sourceConfig));
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        return sourceConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId);
    }
}
