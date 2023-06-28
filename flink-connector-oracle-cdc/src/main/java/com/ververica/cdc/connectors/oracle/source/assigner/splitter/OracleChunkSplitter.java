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

package com.ververica.cdc.connectors.oracle.source.assigner.splitter;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.AbstractJdbcSourceChunkSplitter;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkRange;
import com.ververica.cdc.connectors.base.utils.ObjectUtils;
import com.ververica.cdc.connectors.oracle.source.utils.OracleTypeUtils;
import com.ververica.cdc.connectors.oracle.source.utils.OracleUtils;
import com.ververica.cdc.connectors.oracle.util.ChunkUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import oracle.sql.ROWID;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * The {@code ChunkSplitter} used to split Oracle table into a set of chunks for JDBC data source.
 */
public class OracleChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    private final JdbcSourceConfig sourceConfig;

    public OracleChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
        this.sourceConfig = sourceConfig;
    }

    @Override
    protected Column getSplitColumn(Table table) {
        return ChunkUtils.getChunkKeyColumn(table, sourceConfig.getChunkKeyColumn());
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return OracleUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return OracleUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OracleUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return OracleUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return OracleUtils.buildSplitScanQuery(tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return OracleTypeUtils.fromDbzColumn(splitColumn);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks, using
     * evenly-sized chunks which is much efficient, using unevenly-sized chunks which will request
     * many queries and is not efficient.
     */
    @Override
    protected List<ChunkRange> splitTableIntoChunks(
            JdbcConnection jdbc, TableId tableId, Column splitColumn) throws SQLException {
        final String splitColumnName = splitColumn.name();
        final Object[] minMax = queryMinMax(jdbc, tableId, splitColumnName);
        final Object min = minMax[0];
        final Object max = minMax[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final int chunkSize = sourceConfig.getSplitSize();
        // use ROWID get splitUnevenlySizedChunks by default
        if (splitColumn.name().equals(ROWID.class.getSimpleName())) {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumnName, min, max, chunkSize);
        }

        return super.splitTableIntoChunks(jdbc, tableId, splitColumn);
    }

    /** ChunkEnd less than or equal to max. */
    @Override
    protected boolean isChunkEndLeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            <= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0;
        }
        return chunkEndMaxCompare;
    }

    /** ChunkEnd greater than or equal to max. */
    @Override
    protected boolean isChunkEndGeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            >= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) >= 0;
        }
        return chunkEndMaxCompare;
    }
}
