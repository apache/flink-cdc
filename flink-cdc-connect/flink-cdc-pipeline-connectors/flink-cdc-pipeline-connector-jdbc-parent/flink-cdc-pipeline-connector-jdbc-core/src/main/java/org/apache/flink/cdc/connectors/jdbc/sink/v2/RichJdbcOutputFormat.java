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

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import javax.annotation.Nonnull;

/**
 * Merely a wrapper of {@code JdbcOutputFormat<Object, JdbcRowData,
 * JdbcBatchStatementExecutor<JdbcRowData>>} to avoid typing such a long generic type arguments
 * again and again.
 */
public class RichJdbcOutputFormat
        extends JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>> {
    public RichJdbcOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull
                    StatementExecutorFactory<JdbcBatchStatementExecutor<JdbcRowData>>
                            statementExecutorFactory) {
        super(connectionProvider, executionOptions, statementExecutorFactory);
    }
}
