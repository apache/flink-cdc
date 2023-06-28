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

package com.ververica.cdc.connectors.postgres.source;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.source.assigner.splitter.AbstractJdbcSourceChunkSplitter;
import com.ververica.cdc.connectors.postgres.source.utils.ChunkUtils;
import com.ververica.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import com.ververica.cdc.connectors.postgres.source.utils.PostgresTypeUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;

/**
 * The splitter to split the table into chunks using primary-key (by default) or a given split key.
 */
public class PostgresChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    private final JdbcSourceConfig sourceConfig;

    public PostgresChunkSplitter(JdbcSourceConfig sourceConfig, PostgresDialect postgresDialect) {
        super(sourceConfig, postgresDialect);
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return PostgresQueryUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return PostgresQueryUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return PostgresQueryUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return PostgresQueryUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return PostgresQueryUtils.buildSplitScanQuery(
                tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return PostgresTypeUtils.fromDbzColumn(splitColumn);
    }

    @Override
    protected Column getSplitColumn(Table table) {
        return ChunkUtils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
    }
}
