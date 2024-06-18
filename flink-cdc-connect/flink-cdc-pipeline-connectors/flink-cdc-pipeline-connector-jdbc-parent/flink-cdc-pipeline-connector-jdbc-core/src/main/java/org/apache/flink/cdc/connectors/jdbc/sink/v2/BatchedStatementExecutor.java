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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** A batched statement executor of {@link JdbcRowData}. */
public class BatchedStatementExecutor implements JdbcBatchStatementExecutor<JdbcRowData> {

    private final JdbcBatchStatementExecutor<JdbcRowData> upsertExecutor;
    private final JdbcBatchStatementExecutor<JdbcRowData> deleteExecutor;
    private final Function<JdbcRowData, JdbcRowData> keyExtractor;

    // A buffer holding batched records. The key is the upserting / deleting column row data, and
    // its value could be overwritten.
    private final Map<JdbcRowData, Tuple2<Boolean, JdbcRowData>> reduceBuffer;

    public BatchedStatementExecutor(
            JdbcBatchStatementExecutor<JdbcRowData> upsertExecutor,
            JdbcBatchStatementExecutor<JdbcRowData> deleteExecutor,
            Function<JdbcRowData, JdbcRowData> keyExtractor) {
        this.upsertExecutor = upsertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
        this.reduceBuffer = new HashMap<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(JdbcRowData record) {
        JdbcRowData key = keyExtractor.apply(record);
        boolean flag = changeFlag(record.getRowKind());
        reduceBuffer.put(key, Tuple2.of(flag, record));
    }

    /**
     * Returns true if the row kind is INSERT or UPDATE_AFTER, returns false if the row kind is
     * DELETE or UPDATE_BEFORE.
     */
    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER,"
                                        + " DELETE, but get: %s.",
                                rowKind));
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!reduceBuffer.isEmpty()) {
            for (Map.Entry<JdbcRowData, Tuple2<Boolean, JdbcRowData>> entry :
                    reduceBuffer.entrySet()) {
                if (entry.getValue().f0) {
                    upsertExecutor.addToBatch(entry.getValue().f1);
                } else {
                    // delete by key
                    deleteExecutor.addToBatch(entry.getKey());
                }
            }
            upsertExecutor.executeBatch();
            deleteExecutor.executeBatch();
            reduceBuffer.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        upsertExecutor.closeStatements();
        deleteExecutor.closeStatements();
    }
}
