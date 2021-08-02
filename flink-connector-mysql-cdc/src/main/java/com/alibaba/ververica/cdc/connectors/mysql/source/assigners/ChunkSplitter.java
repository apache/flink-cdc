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

package com.alibaba.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.ververica.cdc.connectors.mysql.schema.MySqlSchema;
import com.alibaba.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.ObjectUtils;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryMin;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryMinMax;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryNextChunkMax;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * The {@code ChunkSplitter}'s task is to split table into a set of chunks or called splits (i.e.
 * {@link MySqlSnapshotSplit}).
 */
class ChunkSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkSplitter.class);

    private final MySqlConnection jdbc;
    private final MySqlSchema mySqlSchema;
    private final int chunkSize;

    public ChunkSplitter(MySqlConnection jdbc, MySqlSchema mySqlSchema, int chunkSize) {
        this.jdbc = jdbc;
        this.mySqlSchema = mySqlSchema;
        this.chunkSize = chunkSize;
    }

    /** Generates all snapshot splits (chunks) for the give table path. */
    public Collection<MySqlSnapshotSplit> generateSplits(TableId tableId) {
        long start = System.currentTimeMillis();

        Table schema = mySqlSchema.getTableSchema(tableId).getTable();
        List<Column> primaryKeys = schema.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            tableId));
        }

        // use first field in primary key as the split key
        Column splitColumn = primaryKeys.get(0);
        final List<ChunkRange> chunks;
        try {
            chunks = splitTableIntoChunks(tableId, splitColumn);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to split chunks for table " + tableId, e);
        }

        // convert chunks into splits
        List<MySqlSnapshotSplit> splits = new ArrayList<>();
        RowType splitType = splitType(splitColumn);
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            MySqlSnapshotSplit split =
                    createSnapshotSplit(
                            tableId, i, splitType, chunk.getChunkStart(), chunk.getChunkEnd());
            splits.add(split);
        }

        long end = System.currentTimeMillis();
        LOG.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                tableId,
                splits.size(),
                Duration.ofMillis(end - start));
        return splits;
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private List<ChunkRange> splitTableIntoChunks(TableId tableId, Column splitColumn)
            throws SQLException {
        final String splitColumnName = splitColumn.name();
        final Object[] minMaxOfSplitColumn = queryMinMax(jdbc, tableId, splitColumnName);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> chunks;
        if (splitColumnEvenlyDistributed(splitColumn)) {
            // use evenly-sized chunks which is much efficient
            chunks = splitEvenlySizedChunks(min, max);
        } else {
            // use unevenly-sized chunks which will request many queries and is not efficient.
            chunks = splitUnevenlySizedChunks(tableId, splitColumnName, min, max);
        }

        return chunks;
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in {@link #chunkSize} step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(Object min, Object max) {
        if (ObjectUtils.compare(ObjectUtils.plus(min, chunkSize), max) > 0) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, chunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            chunkEnd = ObjectUtils.plus(chunkEnd, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    /** Split table into unevenly sized chunks by continuously calculating next chunk max value. */
    private List<ChunkRange> splitUnevenlySizedChunks(
            TableId tableId, String splitColumnName, Object min, Object max) throws SQLException {
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(min, tableId, splitColumnName, max);
        int count = 0;
        while (chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(chunkEnd, tableId, splitColumnName, max);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            Object previousChunkEnd, TableId tableId, String splitColumnName, Object max)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(jdbc, tableId, splitColumnName, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(jdbc, tableId, splitColumnName, chunkEnd);
        }
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    private MySqlSnapshotSplit createSnapshotSplit(
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChange> schema = new HashMap<>();
        schema.put(tableId, mySqlSchema.getTableSchema(tableId));
        return new MySqlSnapshotSplit(
                tableId,
                splitId(tableId, chunkId),
                splitKeyType,
                splitStart,
                splitEnd,
                null,
                schema);
    }

    // ------------------------------------------------------------------------------------------

    /** Checks whether split column is evenly distributed across its range. */
    private static boolean splitColumnEvenlyDistributed(Column splitColumn) {
        // only column is auto-incremental are recognized as evenly distributed.
        // TODO: we may use MAX,MIN,COUNT to calculate the distribution in the future.
        if (splitColumn.isAutoIncremented()) {
            DataType flinkType = MySqlTypeUtils.fromDbzColumn(splitColumn);
            LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();
            // currently, we only support split column with type BIGINT, INT, DECIMAL
            return typeRoot == LogicalTypeRoot.BIGINT
                    || typeRoot == LogicalTypeRoot.INTEGER
                    || typeRoot == LogicalTypeRoot.DECIMAL;
        } else {
            return false;
        }
    }

    private static String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    private static RowType splitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), MySqlTypeUtils.fromDbzColumn(splitColumn)))
                        .getLogicalType();
    }

    private static void maySleep(int count) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
        }
    }
}
