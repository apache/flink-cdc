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

package org.apache.flink.cdc.connectors.db2.source.dialect;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2TypeUtils;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2Utils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

import java.sql.SQLException;

/**
 * The splitter to split the table into chunks using primary-key (by default) or a given split key.
 */
@Internal
public class Db2ChunkSplitter extends JdbcSourceChunkSplitter {

    public Db2ChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        super(sourceConfig, dialect, chunkSplitterState);
    }

    @Override
    protected DataType fromDbzColumn(Column splitColumn) {
        return Db2TypeUtils.fromDbzColumn(splitColumn);
    }

    @Override
    protected Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column column,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return Db2Utils.queryNextChunkMax(
                jdbc, tableId, column.name(), chunkSize, includedLowerBound);
    }

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return Db2Utils.queryApproximateRowCnt(jdbc, tableId);
    }
}
