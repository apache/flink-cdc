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

package com.ververica.cdc.connectors.refactor.refactor.source.dialect;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.dialect.SnapshotEventDialect;
import com.ververica.cdc.connectors.base.source.internal.connection.PooledDataSourceFactory;
import com.ververica.cdc.connectors.base.source.internal.converter.JdbcSourceRecordConverter;
import com.ververica.cdc.connectors.base.source.split.SnapshotSplit;
import com.ververica.cdc.connectors.refactor.refactor.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.refactor.refactor.source.connection.MysqlPooledDataSourceFactory;
import com.ververica.cdc.connectors.refactor.refactor.source.dialect.task.context.MySqlSnapshotReaderContext;
import com.ververica.cdc.connectors.refactor.refactor.source.utils.StatementUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** A MySql Dialect to handle database event during snapshotting phase. */
public class MysqlSnapshotDialect extends SnapshotEventDialect implements MysqlDialect {

    private final MysqlPooledDataSourceFactory mysqlPooledDataSourceFactory;

    public MysqlSnapshotDialect(MysqlPooledDataSourceFactory mysqlPooledDataSourceFactory) {
        this.mysqlPooledDataSourceFactory = mysqlPooledDataSourceFactory;
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return StatementUtils.buildSplitScanQuery(tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return StatementUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return StatementUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return StatementUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return StatementUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public PreparedStatement readTableSplitDataStatement(
            String selectSql, SnapshotContext context) {
        SnapshotSplit snapshotSplit = context.getSnapshotSplit();
        MySqlSourceConfig mySqlSourceConfig = (MySqlSourceConfig) context.getSourceConfig();
        return StatementUtils.readTableSplitDataStatement(
                openJdbcConnection(context.getSourceConfig()),
                selectSql,
                snapshotSplit.getSplitStart() == null,
                snapshotSplit.getSplitEnd() == null,
                snapshotSplit.getSplitStart(),
                snapshotSplit.getSplitEnd(),
                snapshotSplit.getSplitKeyType().getFieldCount(),
                mySqlSourceConfig.getMySqlConnectorConfig().getQueryFetchSize());
    }

    @Override
    public Task createTask(SnapshotContext statefulTaskContext) {
        return new SnapshotEventDialect.SnapshotSplitReadTask(statefulTaskContext);
    }

    @Override
    public List<SourceRecord> normalizedSplitRecords(
            SnapshotSplit currentSnapshotSplit, List<SourceRecord> sourceRecords) {
        return null;
    }

    @Override
    public SnapshotContext createSnapshotContext(SourceConfig sourceConfig) {
        return new MySqlSnapshotReaderContext(sourceConfig);
    }

    @Override
    public void close() {}

    @Override
    public String getName() {
        return null;
    }

    @Override
    public JdbcSourceRecordConverter getSourceRecordConverter(RowType rowType) {
        return null;
    }

    @Override
    public PooledDataSourceFactory getPooledDataSourceFactory() {
        return mysqlPooledDataSourceFactory;
    }
}
