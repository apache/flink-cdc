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

package org.apache.flink.cdc.connectors.oceanbase.source.splitter;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseTypeUtils;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

import java.sql.SQLException;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
public class OceanBaseChunkSplitter extends JdbcSourceChunkSplitter {

    public OceanBaseChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        super(sourceConfig, dialect, chunkSplitterState);
    }

    @Override
    protected Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OceanBaseUtils.queryNextChunkMax(
                jdbc, tableId, splitColumn.name(), chunkSize, includedLowerBound);
    }

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        if (tableId.catalog() != null) {
            return OceanBaseUtils.queryApproximateRowCnt(jdbc, tableId);
        }
        return 0L;
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return OceanBaseTypeUtils.fromDbzColumn(splitColumn);
    }
}
