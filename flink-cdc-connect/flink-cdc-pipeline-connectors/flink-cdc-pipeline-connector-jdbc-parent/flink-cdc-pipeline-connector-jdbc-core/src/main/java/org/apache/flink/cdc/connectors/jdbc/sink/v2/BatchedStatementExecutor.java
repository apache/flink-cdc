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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** A batched statement executor of {@link RichJdbcRowData}. */
public class BatchedStatementExecutor implements JdbcBatchStatementExecutor<RichJdbcRowData> {
    private final String upsertSql;
    private final String deleteSql;
    private final JdbcStatementBuilder<RichJdbcRowData> upsertParamSetter;
    private final JdbcStatementBuilder<RichJdbcRowData> deleteParamSetter;
    private final List<RichJdbcRowData> batch;

    private transient PreparedStatement upsertStmt;
    private transient PreparedStatement deleteStmt;

    public BatchedStatementExecutor(
            String upsertSql,
            String deleteSql,
            JdbcStatementBuilder<RichJdbcRowData> upsertParamSetter,
            JdbcStatementBuilder<RichJdbcRowData> deleteParamSetter) {
        this.upsertSql = upsertSql;
        this.deleteSql = deleteSql;
        this.upsertParamSetter = upsertParamSetter;
        this.deleteParamSetter = deleteParamSetter;
        this.batch = new ArrayList<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.upsertStmt = connection.prepareStatement(this.upsertSql);
        this.deleteStmt = connection.prepareStatement(this.deleteSql);
    }

    @Override
    public void addToBatch(RichJdbcRowData record) {
        this.batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!this.batch.isEmpty()) {
            for (RichJdbcRowData record : this.batch) {
                RowKind rowKind = record.getRowKind();
                PreparedStatement statement;
                JdbcStatementBuilder<RichJdbcRowData> paramSetter;
                if (rowKind.is(RowKind.INSERT) || rowKind.is(RowKind.UPDATE_AFTER)) {
                    statement = this.upsertStmt;
                    paramSetter = this.upsertParamSetter;
                } else if (rowKind.is(RowKind.UPDATE_BEFORE) || rowKind.is(RowKind.DELETE)) {
                    statement = this.deleteStmt;
                    paramSetter = this.deleteParamSetter;
                } else {
                    throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
                }

                paramSetter.accept(statement, record);
                statement.execute();
            }
            batch.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (upsertStmt != null) {
            upsertStmt.close();
            upsertStmt = null;
        }
        if (deleteStmt != null) {
            deleteStmt.close();
            deleteStmt = null;
        }
    }
}
