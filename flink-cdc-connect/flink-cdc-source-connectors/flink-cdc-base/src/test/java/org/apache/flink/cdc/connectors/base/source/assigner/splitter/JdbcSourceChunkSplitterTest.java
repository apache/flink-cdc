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

    /**
     * The JDBC connection is shared by every job on the same JobManager through a bounded pool, so
     * the splitter must not hold one while idle: nothing is acquired in {@code open()}, and the
     * connection is returned as soon as a table has been split.
     */
    @Test
    void testConnectionIsAcquiredLazilyAndReleasedWhenTableIsSplit() throws Exception {
        TableId tableId = TableId.parse("db.schema.table");
        Column idColumn = Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create();
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(idColumn)
                        .setPrimaryKeyNames("id")
                        .create();

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        JdbcDataSourceDialect dialect = Mockito.mock(JdbcDataSourceDialect.class);
        Mockito.when(dialect.openJdbcConnection(Mockito.any())).thenReturn(connection);
        Mockito.when(dialect.queryTableSchema(Mockito.any(), Mockito.eq(tableId)))
                .thenReturn(
                        new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table));

        JdbcSourceConfig sourceConfig = Mockito.mock(JdbcSourceConfig.class);
        Mockito.when(sourceConfig.getSplitSize()).thenReturn(8096);
        Mockito.when(sourceConfig.getDistributionFactorUpper()).thenReturn(1000.0d);
        Mockito.when(sourceConfig.getDistributionFactorLower()).thenReturn(0.05d);

        JdbcSourceChunkSplitter splitter =
                new TestingJdbcSourceChunkSplitter(
                        sourceConfig, dialect, new Object[] {1, 100}, 100L);

        splitter.open();
        Mockito.verify(dialect, Mockito.never()).openJdbcConnection(Mockito.any());

        Collection<SnapshotSplit> splits = splitter.generateSplits(tableId);

        Assertions.assertThat(splits).hasSize(1);
        Assertions.assertThat(splitter.hasNextChunk()).isFalse();
        Mockito.verify(dialect, Mockito.times(1)).openJdbcConnection(Mockito.any());
        Mockito.verify(connection, Mockito.times(1)).close();

        splitter.close();
        splitter.close();
        Mockito.verify(connection, Mockito.times(1)).close();
    }

    /** Minimal testing implementation that stubs out JDBC interactions. */
    private static class TestingJdbcSourceChunkSplitter extends JdbcSourceChunkSplitter {

        @Nullable private final Object nextChunkMaxResult;
        private final Object[] minMax;
        private final long approximateRowCnt;

        TestingJdbcSourceChunkSplitter(@Nullable Object nextChunkMaxResult) {
            super(null, null, null, null, null);
            this.nextChunkMaxResult = nextChunkMaxResult;
            this.minMax = new Object[] {null, null};
            this.approximateRowCnt = 0L;
        }

        TestingJdbcSourceChunkSplitter(
                JdbcSourceConfig sourceConfig,
                JdbcDataSourceDialect dialect,
                Object[] minMax,
                long approximateRowCnt) {
            super(sourceConfig, dialect, null, null, null);
            this.nextChunkMaxResult = null;
            this.minMax = minMax;
            this.approximateRowCnt = approximateRowCnt;
        }

        @Override
        protected Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                Column splitColumn,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return nextChunkMaxResult;
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
