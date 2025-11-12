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

package org.apache.flink.cdc.connectors.base.source.assigner.splitter;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.utils.JdbcChunkUtils;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.math.BigDecimal.ROUND_CEILING;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
@Experimental
public abstract class JdbcSourceChunkSplitter implements ChunkSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceChunkSplitter.class);
    protected final JdbcSourceConfig sourceConfig;
    protected final JdbcDataSourceDialect dialect;

    private final Object lock = new Object();
    @Nullable private TableId currentSplittingTableId;
    @Nullable private ChunkSplitterState.ChunkBound nextChunkStart;
    @Nullable private Integer nextChunkId;

    private JdbcConnection jdbcConnection;
    private Table currentSplittingTable;
    private TableChanges.TableChange currentSchema;
    private Column splitColumn;
    private RowType splitType;
    private Object[] minMaxOfSplitColumn;
    private long approximateRowCnt;

    public JdbcSourceChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        this(
                sourceConfig,
                dialect,
                chunkSplitterState.getCurrentSplittingTableId(),
                chunkSplitterState.getNextChunkStart(),
                chunkSplitterState.getNextChunkId());
    }

    public JdbcSourceChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            @Nullable TableId currentSplittingTableId,
            @Nullable ChunkSplitterState.ChunkBound nextChunkStart,
            @Nullable Integer nextChunkId) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
        this.currentSplittingTableId = currentSplittingTableId;
        this.nextChunkStart = nextChunkStart;
        this.nextChunkId = nextChunkId;
    }

    @Override
    public void open() {
        this.jdbcConnection = dialect.openJdbcConnection(sourceConfig);
    }

    /** Generates all snapshot splits (chunks) for the give table path. */
    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) throws Exception {
        if (!hasNextChunk()) {
            // split a new table.
            analyzeTable(tableId);
            Optional<List<SnapshotSplit>> evenlySplitChunks = trySplitAllEvenlySizedChunks(tableId);
            if (evenlySplitChunks.isPresent()) {
                return evenlySplitChunks.get();
            } else {
                synchronized (lock) {
                    this.currentSplittingTableId = tableId;
                    this.nextChunkStart = ChunkSplitterState.ChunkBound.START_BOUND;
                    this.nextChunkId = 0;
                    return Collections.singletonList(splitOneUnevenlySizedChunk(tableId));
                }
            }
        } else {
            Preconditions.checkState(
                    currentSplittingTableId.equals(tableId),
                    "Can not split a new table before the previous table splitting finish.");
            if (currentSplittingTable == null) {
                analyzeTable(currentSplittingTableId);
            }
            synchronized (lock) {
                return Collections.singletonList(splitOneUnevenlySizedChunk(tableId));
            }
        }
    }

    @Override
    public boolean hasNextChunk() {
        return currentSplittingTableId != null;
    }

    @Override
    public ChunkSplitterState snapshotState(long checkpointId) {
        // don't chunk new spilt when snapshot state.
        synchronized (lock) {
            return new ChunkSplitterState(currentSplittingTableId, nextChunkStart, nextChunkId);
        }
    }

    @Override
    public TableId getCurrentSplittingTableId() {
        return currentSplittingTableId;
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
    }

    /**
     * Query the maximum value of the next chunk, and the next chunk must be greater than or equal
     * to <code>includedLowerBound</code> value [min_1, max_1), [min_2, max_2),... [min_n, null).
     * Each time this method is called it will return max1, max2...
     *
     * <p>Each database has different grammar to get limit number of data, for example, `limit N` in
     * mysql or postgres, `top(N)` in sqlserver , `FETCH FIRST %S ROWS ONLY` in DB2.
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param splitColumn column.
     * @param chunkSize chunk size.
     * @param includedLowerBound the previous chunk end value.
     * @return next chunk end value.
     */
    protected abstract Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException;

    /**
     * Approximate total number of entries in the lookup table.
     *
     * <p>Each database has different system table to lookup up approximate total number. For
     * example, `pg_class` in postgres, `sys.dm_db_partition_stats` in sqlserver, `SYSCAT.TABLE` in
     * db2.
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @return approximate row count.
     */
    protected abstract Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException;

    /**
     * Checks whether split column is evenly distributed across its range.
     *
     * @param splitColumn split column.
     * @return true that means split column with type BIGINT, INT, DECIMAL.
     */
    protected boolean isEvenlySplitColumn(Column splitColumn) {
        DataType flinkType = fromDbzColumn(splitColumn);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();

        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        return typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL;
    }

    /**
     * Get a corresponding Flink data type from a debezium {@link Column}.
     *
     * @param splitColumn dbz split column.
     * @return flink data type
     */
    protected abstract DataType fromDbzColumn(Column splitColumn);

    /** Returns the distribution factor of the given table. */
    protected double calculateDistributionFactor(
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

    /**
     * Get the column which is seen as chunk key.
     *
     * @param table table identity.
     * @param chunkKeyColumns column name which is seen as chunk key, if chunkKeyColumns is null,
     *     use primary key instead.
     * @return the column which is seen as chunk key.
     */
    protected Column getSplitColumn(
            Table table, @Nullable Map<ObjectPath, String> chunkKeyColumns) {
        return JdbcChunkUtils.getSplitColumn(table, chunkKeyColumns);
    }

    /** ChunkEnd less than or equal to max. */
    protected boolean isChunkEndLeMax(
            @Nullable JdbcConnection jdbc, Object chunkEnd, Object max, Column splitColumn)
            throws SQLException {
        return ObjectUtils.compare(chunkEnd, max) <= 0;
    }

    /** ChunkEnd greater than or equal to max. */
    protected boolean isChunkEndGeMax(
            @Nullable JdbcConnection jdbc, Object chunkEnd, Object max, Column splitColumn)
            throws SQLException {
        return ObjectUtils.compare(chunkEnd, max) >= 0;
    }

    /**
     * Query the maximum and minimum value of the column in the table. e.g. query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param splitColumn column.
     * @return maximum and minimum value.
     */
    protected Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
            throws SQLException {
        return JdbcChunkUtils.queryMinMax(
                jdbc,
                jdbc.quotedTableIdString(tableId),
                jdbc.quotedColumnIdString(splitColumn.name()));
    }

    /**
     * Query the minimum value of the column in the table, and the minimum value must greater than
     * the excludedLowerBound value. e.g. prepare query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param splitColumn column.
     * @param excludedLowerBound the minimum value should be greater than this value.
     * @return minimum value.
     */
    protected Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column splitColumn, Object excludedLowerBound)
            throws SQLException {
        return JdbcChunkUtils.queryMin(
                jdbc,
                jdbc.quotedTableIdString(tableId),
                jdbc.quotedColumnIdString(splitColumn.name()),
                excludedLowerBound);
    }

    /**
     * convert dbz column to Flink row type.
     *
     * @param splitColumn split column.
     * @return flink row type.
     */
    private RowType getSplitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), fromDbzColumn(splitColumn))).getLogicalType();
    }

    /**
     * Try to split all chunks for evenly-sized table, or else return empty.
     *
     * <p>We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks,
     * using evenly-sized chunks which is much efficient, using unevenly-sized chunks which will
     * request many queries and is not efficient.
     */
    private Optional<List<SnapshotSplit>> trySplitAllEvenlySizedChunks(TableId tableId) {
        LOG.debug("Try evenly splitting table {} into chunks", tableId);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Optional.of(
                    createSnapshotSplit(tableId, Collections.singletonList(ChunkRange.all())));
        }

        final int chunkSize = sourceConfig.getSplitSize();
        final int dynamicChunkSize =
                getDynamicChunkSize(tableId, splitColumn, min, max, chunkSize, approximateRowCnt);
        if (dynamicChunkSize != -1) {
            LOG.debug("finish evenly splitting table {} into chunks", tableId);
            List<ChunkRange> chunks =
                    splitEvenlySizedChunks(
                            tableId, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
            return Optional.of(createSnapshotSplit(tableId, chunks));
        } else {
            LOG.debug("beginning unevenly splitting table {} into chunks", tableId);
            return Optional.empty();
        }
    }

    /** Analyze the meta information for given table. */
    private void analyzeTable(TableId tableId) {
        try {
            currentSchema = dialect.queryTableSchema(jdbcConnection, tableId);
            currentSplittingTable = Objects.requireNonNull(currentSchema).getTable();
            splitColumn = getSplitColumn(currentSplittingTable, sourceConfig.getChunkKeyColumns());
            splitType = getSplitType(splitColumn);
            minMaxOfSplitColumn = queryMinMax(jdbcConnection, tableId, splitColumn);
            approximateRowCnt = queryApproximateRowCnt(jdbcConnection, tableId);
        } catch (Exception e) {
            throw new RuntimeException("Fail to analyze table in chunk splitter.", e);
        }
    }

    /** Generates one snapshot split (chunk) for the give table path. */
    private SnapshotSplit splitOneUnevenlySizedChunk(TableId tableId) throws SQLException {
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
                        splitColumn,
                        minMaxOfSplitColumn[1],
                        chunkSize);
        // may sleep a while to avoid DDOS on MySQL server
        maySleep(nextChunkId, tableId);
        if (chunkEnd != null
                && isChunkEndLeMax(jdbcConnection, chunkEnd, minMaxOfSplitColumn[1], splitColumn)) {
            nextChunkStart = ChunkSplitterState.ChunkBound.middleOf(chunkEnd);
            return createSnapshotSplit(tableId, nextChunkId++, splitType, chunkStartVal, chunkEnd);
        } else {
            currentSplittingTableId = null;
            nextChunkStart = ChunkSplitterState.ChunkBound.END_BOUND;
            return createSnapshotSplit(tableId, nextChunkId++, splitType, chunkStartVal, null);
        }
    }

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
        if (!isEvenlySplitColumn(splitColumn)) {
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

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(
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
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    /** Split table into unevenly sized chunks by continuously calculating next chunk max value. */
    private List<ChunkRange> splitUnevenlySizedChunks(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            Object min,
            Object max,
            int chunkSize)
            throws SQLException {
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}", tableId, chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(jdbc, min, tableId, splitColumn, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && isChunkEndLeMax(jdbcConnection, chunkEnd, max, splitColumn)) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on PostgreSQL server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumn, max, chunkSize);
        }
        // add the ending split
        splits.add(0, ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            Column splitColumn,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(jdbc, tableId, splitColumn, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(jdbc, tableId, splitColumn, chunkEnd);
        }
        if (isChunkEndGeMax(jdbc, chunkEnd, max, splitColumn)) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    /** Generates all snapshot splits (chunks) from chunk ranges. */
    private List<SnapshotSplit> createSnapshotSplit(TableId tableId, List<ChunkRange> chunks) {
        // convert chunks into splits
        List<SnapshotSplit> splits = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            SnapshotSplit split =
                    createSnapshotSplit(
                            tableId, i, splitType, chunk.getChunkStart(), chunk.getChunkEnd());
            splits.add(split);
        }
        return splits;
    }

    private SnapshotSplit createSnapshotSplit(
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        schema.put(tableId, currentSchema);
        return new SnapshotSplit(
                tableId, chunkId, splitKeyType, splitStart, splitEnd, null, schema);
    }

    private void maySleep(int count, TableId tableId) {
        // every 10 queries to sleep 0.1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }
}
