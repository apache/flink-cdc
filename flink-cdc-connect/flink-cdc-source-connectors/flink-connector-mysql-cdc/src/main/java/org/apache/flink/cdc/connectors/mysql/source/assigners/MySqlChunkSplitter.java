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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTypeUtils;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.math.BigDecimal.ROUND_CEILING;

/** The {@link ChunkSplitter} implementation for MySQL. */
public class MySqlChunkSplitter implements ChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlChunkSplitter.class);
    private final Object lock = new Object();

    private final MySqlSourceConfig sourceConfig;
    private final MySqlSchema mySqlSchema;

    @Nullable private TableId currentSplittingTableId;
    @Nullable private ChunkSplitterState.ChunkBound nextChunkStart;
    @Nullable private Integer nextChunkId;

    private JdbcConnection jdbcConnection;
    private Table currentSplittingTable;
    private Column splitColumn;
    private RowType splitType;
    private Object[] minMaxOfSplitColumn;
    private long approximateRowCnt;

    public MySqlChunkSplitter(MySqlSchema mySqlSchema, MySqlSourceConfig sourceConfig) {
        this(mySqlSchema, sourceConfig, null, null, null);
    }

    public MySqlChunkSplitter(
            MySqlSchema mySqlSchema,
            MySqlSourceConfig sourceConfig,
            ChunkSplitterState chunkSplitterState) {
        this(
                mySqlSchema,
                sourceConfig,
                chunkSplitterState.getCurrentSplittingTableId(),
                chunkSplitterState.getNextChunkStart(),
                chunkSplitterState.getNextChunkId());
    }

    private MySqlChunkSplitter(
            MySqlSchema mySqlSchema,
            MySqlSourceConfig sourceConfig,
            @Nullable TableId currentSplittingTableId,
            @Nullable ChunkSplitterState.ChunkBound nextChunkStart,
            @Nullable Integer nextChunkId) {
        this.mySqlSchema = mySqlSchema;
        this.sourceConfig = sourceConfig;
        this.currentSplittingTableId = currentSplittingTableId;
        this.nextChunkStart = nextChunkStart;
        this.nextChunkId = nextChunkId;
    }

    @Override
    public void open() {
        this.jdbcConnection = DebeziumUtils.openJdbcConnection(sourceConfig);
    }

    @Override
    public List<MySqlSnapshotSplit> splitChunks(MySqlPartition partition, TableId tableId)
            throws Exception {
        if (!hasNextChunk()) {
            analyzeTable(partition, tableId);
            Optional<List<MySqlSnapshotSplit>> evenlySplitChunks =
                    trySplitAllEvenlySizedChunks(partition, tableId);
            if (evenlySplitChunks.isPresent()) {
                return evenlySplitChunks.get();
            } else {
                synchronized (lock) {
                    this.currentSplittingTableId = tableId;
                    this.nextChunkStart = ChunkSplitterState.ChunkBound.START_BOUND;
                    this.nextChunkId = 0;
                    return Collections.singletonList(
                            splitOneUnevenlySizedChunk(partition, tableId));
                }
            }
        } else {
            Preconditions.checkState(
                    currentSplittingTableId.equals(tableId),
                    "Can not split a new table before the previous table splitting finish.");
            if (currentSplittingTable == null) {
                analyzeTable(partition, currentSplittingTableId);
            }
            synchronized (lock) {
                return Collections.singletonList(splitOneUnevenlySizedChunk(partition, tableId));
            }
        }
    }

    /** Analyze the meta information for given table. */
    private void analyzeTable(MySqlPartition partition, TableId tableId) {
        try {
            currentSplittingTable =
                    mySqlSchema.getTableSchema(partition, jdbcConnection, tableId).getTable();
            splitColumn =
                    ChunkUtils.getChunkKeyColumn(
                            currentSplittingTable, sourceConfig.getChunkKeyColumns());
            splitType =
                    ChunkUtils.getChunkKeyColumnType(
                            splitColumn, sourceConfig.isTreatTinyInt1AsBoolean());
            minMaxOfSplitColumn =
                    StatementUtils.queryMinMax(jdbcConnection, tableId, splitColumn.name());
            approximateRowCnt = StatementUtils.queryApproximateRowCnt(jdbcConnection, tableId);
        } catch (Exception e) {
            throw new RuntimeException("Fail to analyze table in chunk splitter.", e);
        }
    }

    /** Generates one snapshot split (chunk) for the give table path. */
    private MySqlSnapshotSplit splitOneUnevenlySizedChunk(MySqlPartition partition, TableId tableId)
            throws SQLException {
        final int chunkSize = sourceConfig.getSplitSize();
        final Object chunkStartVal = nextChunkStart.getValue();
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {} from {}",
                tableId,
                chunkSize,
                nextChunkStart == ChunkSplitterState.ChunkBound.START_BOUND
                        ? "null"
                        : chunkStartVal.toString());
        // we start from [null, min + chunk_size) and avoid [null, min)
        Object chunkEnd =
                nextChunkEnd(
                        jdbcConnection,
                        nextChunkStart == ChunkSplitterState.ChunkBound.START_BOUND
                                ? minMaxOfSplitColumn[0]
                                : chunkStartVal,
                        tableId,
                        splitColumn.name(),
                        minMaxOfSplitColumn[1],
                        chunkSize);
        // may sleep a while to avoid DDOS on MySQL server
        maySleep(nextChunkId, tableId);
        if (chunkEnd != null && ObjectUtils.compare(chunkEnd, minMaxOfSplitColumn[1]) <= 0) {
            nextChunkStart = ChunkSplitterState.ChunkBound.middleOf(chunkEnd);
            return createSnapshotSplit(
                    jdbcConnection,
                    partition,
                    tableId,
                    nextChunkId++,
                    splitType,
                    chunkStartVal,
                    chunkEnd);
        } else {
            currentSplittingTableId = null;
            nextChunkStart = ChunkSplitterState.ChunkBound.END_BOUND;
            return createSnapshotSplit(
                    jdbcConnection,
                    partition,
                    tableId,
                    nextChunkId++,
                    splitType,
                    chunkStartVal,
                    null);
        }
    }

    /**
     * Try to split all chunks for evenly-sized table, or else return empty.
     *
     * <p>We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks,
     * using evenly-sized chunks which is much efficient, using unevenly-sized chunks which will
     * request many queries and is not efficient.
     */
    private Optional<List<MySqlSnapshotSplit>> trySplitAllEvenlySizedChunks(
            MySqlPartition partition, TableId tableId) {
        LOG.debug("Try evenly splitting table {} into chunks", tableId);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Optional.of(
                    generateSplits(
                            partition, tableId, Collections.singletonList(ChunkRange.all())));
        }

        final int chunkSize = sourceConfig.getSplitSize();
        final int dynamicChunkSize =
                getDynamicChunkSize(tableId, splitColumn, min, max, chunkSize, approximateRowCnt);
        if (dynamicChunkSize != -1) {
            LOG.debug("finish evenly splitting table {} into chunks", tableId);
            List<ChunkRange> chunks =
                    splitEvenlySizedChunks(
                            tableId, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
            return Optional.of(generateSplits(partition, tableId, chunks));
        } else {
            LOG.debug("beginning unevenly splitting table {} into chunks", tableId);
            return Optional.empty();
        }
    }

    /** Generates all snapshot splits (chunks) from chunk ranges. */
    private List<MySqlSnapshotSplit> generateSplits(
            MySqlPartition partition, TableId tableId, List<ChunkRange> chunks) {
        // convert chunks into splits
        List<MySqlSnapshotSplit> splits = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            MySqlSnapshotSplit split =
                    createSnapshotSplit(
                            jdbcConnection,
                            partition,
                            tableId,
                            i,
                            splitType,
                            chunk.getChunkStart(),
                            chunk.getChunkEnd());
            splits.add(split);
        }
        return splits;
    }

    @Override
    public boolean hasNextChunk() {
        return currentSplittingTableId != null;
    }

    @Override
    public ChunkSplitterState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new ChunkSplitterState(currentSplittingTableId, nextChunkStart, nextChunkId);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    @VisibleForTesting
    public List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        LOG.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tableId,
                approximateRowCnt,
                chunkSize,
                dynamicChunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, dynamicChunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            try {
                chunkEnd = ObjectUtils.plus(chunkEnd, dynamicChunkSize);
            } catch (ArithmeticException e) {
                // Stop chunk split to avoid dead loop when number overflows.
                break;
            }
        }
        // add the ending split
        // assign ending split first, both the largest and smallest unbounded chunks are completed
        // in the first two splits
        if (sourceConfig.isAssignEndingChunkFirst()) {
            splits.add(0, ChunkRange.of(chunkStart, null));
        } else {
            splits.add(ChunkRange.of(chunkStart, null));
        }
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
                StatementUtils.queryNextChunkMax(
                        jdbc, tableId, splitColumnName, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = StatementUtils.queryMin(jdbc, tableId, splitColumnName, chunkEnd);

            // queryMin will return null when the chunkEnd is the max value,
            // this will happen when the mysql table ignores the capitalization.
            // see more detail at the test MySqlConnectorITCase#testReadingWithMultiMaxValue.
            // In the test, the max value of order_id will return 'e' and when we get the chunkEnd =
            // 'E',
            // this method will return 'E' and will not return null.
            // When this method is invoked next time, queryMin will return null here.
            // So we need return null when we reach the max value here.
            if (chunkEnd == null) {
                return null;
            }
        }
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    private MySqlSnapshotSplit createSnapshotSplit(
            JdbcConnection jdbc,
            MySqlPartition partition,
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChange> schema = new HashMap<>();
        schema.put(tableId, mySqlSchema.getTableSchema(partition, jdbc, tableId));
        return new MySqlSnapshotSplit(
                tableId, chunkId, splitKeyType, splitStart, splitEnd, null, schema);
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Checks whether split column is evenly distributed across its range and return the
     * dynamicChunkSize. If the split column is not evenly distributed, return -1.
     */
    private int getDynamicChunkSize(
            TableId tableId,
            Column splitColumn,
            Object min,
            Object max,
            int chunkSize,
            long approximateRowCnt) {
        if (!isEvenlySplitColumn(splitColumn, sourceConfig.isTreatTinyInt1AsBoolean())) {
            return -1;
        }
        final double distributionFactorUpper = sourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = sourceConfig.getDistributionFactorLower();
        double distributionFactor =
                calculateDistributionFactor(tableId, min, max, approximateRowCnt);
        boolean dataIsEvenlyDistributed =
                ObjectUtils.doubleCompare(distributionFactor, distributionFactorLower) >= 0
                        && ObjectUtils.doubleCompare(distributionFactor, distributionFactorUpper)
                                <= 0;
        LOG.info(
                "The actual distribution factor for table {} is {}, the lower bound of evenly distribution factor is {}, the upper bound of evenly distribution factor is {}",
                tableId,
                distributionFactor,
                distributionFactorLower,
                distributionFactorUpper);
        if (dataIsEvenlyDistributed) {
            // the minimum dynamic chunk size is at least 1
            return Math.max((int) (distributionFactor * chunkSize), 1);
        }
        return -1;
    }

    /** Checks whether split column is evenly distributed across its range. */
    private static boolean isEvenlySplitColumn(Column splitColumn, boolean tinyInt1isBit) {
        DataType flinkType = MySqlTypeUtils.fromDbzColumn(splitColumn, tinyInt1isBit);
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

    private static void maySleep(int count, TableId tableId) {
        // every 10 queries to sleep 0.1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("ChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    public TableId getCurrentSplittingTableId() {
        return currentSplittingTableId;
    }

    public Integer getNextChunkId() {
        return nextChunkId;
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        mySqlSchema.close();
    }
}
