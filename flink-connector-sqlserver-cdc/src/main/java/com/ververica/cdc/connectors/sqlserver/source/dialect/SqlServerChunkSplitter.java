/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.source.dialect;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.AbstractJdbcSourceChunkSplitter;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerTypeUtils;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;

/**
 * The {@code ChunkSplitter} used to split SqlServer table into a set of chunks for JDBC data
 * source.
 */
public class SqlServerChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    public SqlServerChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
    }

    @Override
    protected Column getSplitColumn(Table table) {
        return SqlServerUtils.getSplitColumn(table);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return SqlServerUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return SqlServerUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return SqlServerTypeUtils.fromDbzColumn(splitColumn);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return SqlServerUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return SqlServerUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return SqlServerUtils.buildSplitScanQuery(tableId, splitKeyType, isFirstSplit, isLastSplit);
    }
}
