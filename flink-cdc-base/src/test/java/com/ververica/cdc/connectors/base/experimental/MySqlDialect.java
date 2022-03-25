/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.experimental;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.experimental.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** The {@link JdbcDataSourceDialect} implementation for MySQL datasource. */
public class MySqlDialect implements JdbcDataSourceDialect {

    private final MySqlSourceConfigFactory configFactory;

    public MySqlDialect(MySqlSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public String getName() {
        return null;
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
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return null;
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        return null;
    }

    @Override
    public OffsetContext getOffsetContext(
            JdbcSourceConfig sourceConfig, SourceSplitBase splitBase) {
        return null;
    }

    @Override
    public JdbcSourceEventDispatcher getEventDispatcher(
            JdbcSourceConfig sourceConfig, SourceSplitBase splitBase) {
        return null;
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return null;
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return new Object[0];
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return null;
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return null;
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return null;
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        return null;
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return null;
    }
}
