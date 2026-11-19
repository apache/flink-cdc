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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.table.api.DataTypes;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

/** Tests for {@link JdbcSourceChunkSplitter}. */
class JdbcSourceChunkSplitterTest {

    private static final TableId TABLE_ID = TableId.parse("db.schema.table");

    /**
     * Guards against regressions where {@code queryNextChunkMax} may return {@code null} (e.g. when
     * the max row was removed after MIN/MAX was determined). {@code nextChunkEnd} must handle this
     * gracefully and return null without throwing exceptions.
     */
    @Test
    void testNextChunkEndReturnsNullWhenMaxRowRemoved() throws Exception {
        // given a splitter whose queryNextChunkMax always returns null
        JdbcSourceChunkSplitter splitter = new TestingJdbcSourceChunkSplitter(null);

        TableId tableId = TableId.parse("catalog.db.table");
        Column splitColumn =
                Column.editor().name("id").type("INT").jdbcType(java.sql.Types.INTEGER).create();

        Object previousChunkEnd = 10;
        Object max = 100;
        int chunkSize = 5;

        // when queryNextChunkMax returns null, nextChunkEnd should also return null
        Object result =
                splitter.nextChunkEnd(null, previousChunkEnd, tableId, splitColumn, max, chunkSize);

        Assertions.assertThat(result).isNull();
    }

    /** The splitter must not hold a JDBC connection while idle, before or after splitting. */
    @Test
    void testConnectionIsReleasedWhenEvenlySizedTableIsSplit() throws Exception {
        // 100 rows in [1, 100] is evenly distributed and fits in one chunk
        Fixture fixture = new Fixture(new Object[] {1, 100}, 100L);

        fixture.splitter.open();
        Mockito.verify(fixture.dialect, Mockito.never()).openJdbcConnection(Mockito.any());

        Collection<SnapshotSplit> splits = fixture.splitter.generateSplits(TABLE_ID);

        Assertions.assertThat(splits).hasSize(1);
        Assertions.assertThat(fixture.splitter.hasNextChunk()).isFalse();
        Mockito.verify(fixture.dialect, Mockito.times(1)).openJdbcConnection(Mockito.any());
        Mockito.verify(fixture.connection, Mockito.times(1)).close();
    }

    /**
     * An unevenly distributed table is split one chunk per {@code generateSplits} call; each call
     * must release its connection instead of holding it until the table is fully split.
     */
    @Test
    void testConnectionIsReleasedAfterEachUnevenlySizedChunk() throws Exception {
        // 10 rows in [1, 1000000] exceeds the upper distribution factor, so chunks are queried
        // one by one: the first query ends at 10, the second finds no more rows
        Fixture fixture = new Fixture(new Object[] {1, 1_000_000}, 10L, 10, null);

        Collection<SnapshotSplit> firstSplits = fixture.splitter.generateSplits(TABLE_ID);

        Assertions.assertThat(firstSplits).hasSize(1);
        Assertions.assertThat(fixture.splitter.hasNextChunk()).isTrue();
        Mockito.verify(fixture.dialect, Mockito.times(1)).openJdbcConnection(Mockito.any());
        Mockito.verify(fixture.connection, Mockito.times(1)).close();

        Collection<SnapshotSplit> lastSplits = fixture.splitter.generateSplits(TABLE_ID);

        Assertions.assertThat(lastSplits).hasSize(1);
        Assertions.assertThat(fixture.splitter.hasNextChunk()).isFalse();
        Mockito.verify(fixture.dialect, Mockito.times(2)).openJdbcConnection(Mockito.any());
        Mockito.verify(fixture.connection, Mockito.times(2)).close();
    }

    /** A splitter over a single-column table whose dialect and connection are Mockito mocks. */
    private static class Fixture {
        private final JdbcDataSourceDialect dialect = Mockito.mock(JdbcDataSourceDialect.class);
        private final JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        private final JdbcSourceChunkSplitter splitter;

        Fixture(Object[] minMax, long approximateRowCnt, Object... nextChunkMaxResults) {
            Column idColumn =
                    Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create();
            Table table =
                    Table.editor()
                            .tableId(TABLE_ID)
                            .addColumn(idColumn)
                            .setPrimaryKeyNames("id")
                            .create();
            Mockito.when(dialect.openJdbcConnection(Mockito.any())).thenReturn(connection);
            Mockito.when(dialect.queryTableSchema(Mockito.any(), Mockito.eq(TABLE_ID)))
                    .thenReturn(
                            new TableChanges.TableChange(
                                    TableChanges.TableChangeType.CREATE, table));

            JdbcSourceConfig sourceConfig = Mockito.mock(JdbcSourceConfig.class);
            Mockito.when(sourceConfig.getSplitSize()).thenReturn(8096);
            Mockito.when(sourceConfig.getDistributionFactorUpper()).thenReturn(1000.0d);
            Mockito.when(sourceConfig.getDistributionFactorLower()).thenReturn(0.05d);

            splitter =
                    new TestingJdbcSourceChunkSplitter(
                            sourceConfig, dialect, minMax, approximateRowCnt, nextChunkMaxResults);
        }
    }

    /** Minimal testing implementation that stubs out JDBC interactions. */
    private static class TestingJdbcSourceChunkSplitter extends JdbcSourceChunkSplitter {

        private final Object[] minMax;
        private final long approximateRowCnt;
        private final Object[] nextChunkMaxResults;
        private int nextChunkMaxCalls;

        TestingJdbcSourceChunkSplitter(@Nullable Object nextChunkMaxResult) {
            this(null, null, new Object[] {null, null}, 0L, new Object[] {nextChunkMaxResult});
        }

        TestingJdbcSourceChunkSplitter(
                JdbcSourceConfig sourceConfig,
                JdbcDataSourceDialect dialect,
                Object[] minMax,
                long approximateRowCnt,
                Object[] nextChunkMaxResults) {
            super(sourceConfig, dialect, null, null, null);
            this.minMax = minMax;
            this.approximateRowCnt = approximateRowCnt;
            this.nextChunkMaxResults = nextChunkMaxResults;
        }

        /** Returns the configured results in order, then {@code null} once they are used up. */
        @Override
        protected Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                Column splitColumn,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return nextChunkMaxCalls < nextChunkMaxResults.length
                    ? nextChunkMaxResults[nextChunkMaxCalls++]
                    : null;
        }

        @Override
        protected Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
                throws SQLException {
            return minMax;
        }

        @Override
        protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return approximateRowCnt;
        }

        @Override
        protected Object queryMin(
                JdbcConnection jdbc, TableId tableId, Column splitColumn, Object excludedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        protected org.apache.flink.table.types.DataType fromDbzColumn(Column splitColumn) {
            // The concrete type is irrelevant for this test; just return a simple numeric type.
            return DataTypes.BIGINT();
        }
    }
}
