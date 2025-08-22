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

package org.apache.flink.cdc.connectors.oracle.source.assigner.splitter;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.utils.JdbcChunkUtils;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleTypeUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils;
import org.apache.flink.cdc.connectors.oracle.util.ChunkUtils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import oracle.sql.ROWID;

import javax.annotation.Nullable;

import java.sql.SQLException;

/**
 * The {@code ChunkSplitter} used to split Oracle table into a set of chunks for JDBC data source.
 */
@Internal
public class OracleChunkSplitter extends JdbcSourceChunkSplitter {

    public OracleChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        super(sourceConfig, dialect, chunkSplitterState);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
            throws SQLException {
        // oracle query only use schema and table.
        String quoteSchemaAndTable = OracleUtils.quoteSchemaAndTable(tableId);
        return JdbcChunkUtils.queryMinMax(
                jdbc, quoteSchemaAndTable, jdbc.quotedColumnIdString(splitColumn.name()));
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        // oracle query only use schema and table.
        String quoteSchemaAndTable = OracleUtils.quoteSchemaAndTable(tableId);
        return JdbcChunkUtils.queryMin(
                jdbc, quoteSchemaAndTable, jdbc.quotedTableIdString(tableId), excludedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OracleUtils.queryNextChunkMax(
                jdbc, tableId, splitColumn.name(), chunkSize, includedLowerBound);
    }

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return OracleUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return OracleTypeUtils.fromDbzColumn(splitColumn);
    }

    @Override
    protected boolean isEvenlySplitColumn(Column splitColumn) {
        // use ROWID get splitUnevenlySizedChunks by default
        if (splitColumn.name().equals(ROWID.class.getSimpleName())) {
            return false;
        }

        return super.isEvenlySplitColumn(splitColumn);
    }

    /** ChunkEnd less than or equal to max. */
    @Override
    protected boolean isChunkEndLeMax(
            JdbcConnection jdbc, Object chunkEnd, Object max, Column splitColumn)
            throws SQLException {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            String query =
                    String.format(
                            "SELECT CHARTOROWID(?) ROWIDS FROM DUAL UNION SELECT CHARTOROWID(?) ROWIDS FROM DUAL ORDER BY ROWIDS ASC");
            return jdbc.prepareQueryAndMap(
                    query,
                    ps -> {
                        ps.setObject(1, chunkEnd.toString());
                        ps.setObject(2, max.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            Object obj = rs.getObject(1);
                            return obj.toString().equals(chunkEnd.toString())
                                    || chunkEnd.toString().equals(max.toString());
                        } else {
                            throw new RuntimeException("compare rowid error");
                        }
                    });
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0;
        }
        return chunkEndMaxCompare;
    }

    /** ChunkEnd greater than or equal to max. */
    @Override
    protected boolean isChunkEndGeMax(
            JdbcConnection jdbc, Object chunkEnd, Object max, Column splitColumn)
            throws SQLException {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            String query =
                    String.format(
                            "SELECT CHARTOROWID(?) ROWIDS FROM DUAL UNION SELECT CHARTOROWID(?) ROWIDS FROM DUAL ORDER BY ROWIDS DESC");
            return jdbc.prepareQueryAndMap(
                    query,
                    ps -> {
                        ps.setObject(1, chunkEnd.toString());
                        ps.setObject(2, max.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            Object obj = rs.getObject(1);
                            return obj.toString().equals(chunkEnd.toString())
                                    || chunkEnd.toString().equals(max.toString());
                        } else {
                            throw new RuntimeException("compare rowid error");
                        }
                    });
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) >= 0;
        }
        return chunkEndMaxCompare;
    }

    @Override
    protected Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        // Use the ROWID column as the chunk key column by default for oracle cdc connector
        return ChunkUtils.getChunkKeyColumn(table, chunkKeyColumn);
    }
}
