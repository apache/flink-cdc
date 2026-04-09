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

import org.apache.flink.table.api.DataTypes;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.sql.SQLException;

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

    /** Minimal testing implementation that stubs out JDBC interactions. */
    private static class TestingJdbcSourceChunkSplitter extends JdbcSourceChunkSplitter {

        @Nullable private final Object nextChunkMaxResult;

        TestingJdbcSourceChunkSplitter(@Nullable Object nextChunkMaxResult) {
            super(null, null, null, null, null);
            this.nextChunkMaxResult = nextChunkMaxResult;
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
        protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return 0L;
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
