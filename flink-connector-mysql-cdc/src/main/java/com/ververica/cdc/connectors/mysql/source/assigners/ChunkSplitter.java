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

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.schema.MySqlSchema;
import com.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils;
import com.ververica.cdc.connectors.mysql.source.utils.ObjectUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static com.ververica.cdc.connectors.mysql.source.utils.ObjectUtils.doubleCompare;
import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryApproximateRowCnt;
import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryMin;
import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryMinMax;
import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.queryNextChunkMax;
import static java.math.BigDecimal.ROUND_CEILING;

/**
 * The {@code ChunkSplitter}'s task is to split table into a set of chunks or called splits (i.e.
 * {@link MySqlSnapshotSplit}).
 */
class ChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkSplitter.class);

    private final MySqlSourceConfig sourceConfig;
    private final MySqlSchema mySqlSchema;

    public ChunkSplitter(MySqlSchema mySqlSchema, MySqlSourceConfig sourceConfig) {
        this.mySqlSchema = mySqlSchema;
        this.sourceConfig = sourceConfig;
    }

    /** Generates all snapshot splits (chunks) for the give table path. */
    public Collection<MySqlSnapshotSplit> generateSplits(TableId tableId) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {

            LOG.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            Table table = mySqlSchema.getTableSchema(jdbc, tableId).getTable();
            Column splitColumn = ChunkUtils.getSplitColumn(table);
            final List<ChunkRange> chunks;
            try {
                chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to split chunks for table " + tableId, e);
            }

            // convert chunks into splits
            List<MySqlSnapshotSplit> splits = new ArrayList<>();
            RowType splitType = ChunkUtils.getSplitType(splitColumn);
            for (int i = 0; i < chunks.size(); i++) {
                ChunkRange chunk = chunks.get(i);
                MySqlSnapshotSplit split =
                        createSnapshotSplit(
                                jdbc,
                                tableId,
                                i,
                                splitType,
                                chunk.getChunkStart(),
                                chunk.getChunkEnd());
                splits.add(split);
            }

            long end = System.currentTimeMillis();
            LOG.info(
                    "Split table {} into {} chunks, time cost: {}ms.",
                    tableId,
                    splits.size(),
                    end - start);
            return splits;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Generate Splits for table %s error", tableId), e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks, using
     * evenly-sized chunks which is much efficient, using unevenly-sized chunks which will request
     * many queries and is not efficient.
     */
    private List<ChunkRange> splitTableIntoChunks(
            JdbcConnection jdbc, TableId tableId, Column splitColumn) throws SQLException {
        final String splitColumnName = splitColumn.name();
        final Object[] minMaxOfSplitColumn = queryMinMax(jdbc, tableId, splitColumnName);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final int chunkSize = sourceConfig.getSplitSize();
        final double distributionFactorUpper = sourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = sourceConfig.getDistributionFactorLower();

        if (isEvenlySplitColumn(splitColumn)) {
            long approximateRowCnt = queryApproximateRowCnt(jdbc, tableId);
            double distributionFactor =
                    calculateDistributionFactor(tableId, min, max, approximateRowCnt);

            boolean dataIsEvenlyDistributed =
                    doubleCompare(distributionFactor, distributionFactorLower) >= 0
                            && doubleCompare(distributionFactor, distributionFactorUpper) <= 0;

            if (dataIsEvenlyDistributed) {
                // the minimum dynamic chunk size is at least 1
                final int dynamicChunkSize = Math.max((int) (distributionFactor * chunkSize), 1);
                return splitEvenlySizedChunks(
                        tableId, min, max, approximateRowCnt, dynamicChunkSize);
            } else {
                return splitUnevenlySizedChunks(
                        jdbc, tableId, splitColumnName, min, max, chunkSize);
            }
        } else {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumnName, min, max, chunkSize);
        }
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId, Object min, Object max, long approximateRowCnt, int chunkSize) {
        LOG.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}",
                tableId,
                approximateRowCnt,
                chunkSize);
        if (approximateRowCnt <= chunkSize) {
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
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            Object min,
            Object max,
            int chunkSize)
            throws SQLException {
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}", tableId, chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(jdbc, min, tableId, splitColumnName, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumnName, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            String splitColumnName,
            Object max,
            int chunkSize)
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
            JdbcConnection jdbc,
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChange> schema = new HashMap<>();
        schema.put(tableId, mySqlSchema.getTableSchema(jdbc, tableId));
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
    private static boolean isEvenlySplitColumn(Column splitColumn) {
        DataType flinkType = MySqlTypeUtils.fromDbzColumn(splitColumn);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();

        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        return typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL;
    }

    /** Returns the distribution factor of the given table. */
    private double calculateDistributionFactor(
            TableId tableId, Object min, Object max, long approximateRowCnt) {

        if (!min.getClass().equals(max.getClass())) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported operation type, the MIN value type %s is different with MAX value type %s.",
                            min.getClass().getSimpleName(), max.getClass().getSimpleName()));
        }
        if (approximateRowCnt == 0) {
            return Double.MAX_VALUE;
        }
        BigDecimal difference = ObjectUtils.minus(max, min);
        // factor = (max - min + 1) / rowCount
        final BigDecimal subRowCnt = difference.add(BigDecimal.valueOf(1));
        double distributionFactor =
                subRowCnt.divide(new BigDecimal(approximateRowCnt), 4, ROUND_CEILING).doubleValue();
        LOG.info(
                "The distribution factor of table {} is {} according to the min split key {}, max split key {} and approximate row count {}",
                tableId,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    private static String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    private static void maySleep(int count, TableId tableId) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("ChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }
}
