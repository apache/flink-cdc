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

import com.ververica.cdc.connectors.base.source.dialect.StreamingEventDialect;
import com.ververica.cdc.connectors.base.source.internal.connection.PooledDataSourceFactory;
import com.ververica.cdc.connectors.base.source.internal.converter.JdbcSourceRecordConverter;
import com.ververica.cdc.connectors.base.source.split.StreamSplit;

/** A Mysql Streaming Dialect to handle database event during streaming process. */
public class MysqlStreamingDialect extends StreamingEventDialect implements MysqlDialect {
    @Override
    public Task createTask(StreamSplit backfillStreamSplit) {
        return null;
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
        return null;
    }
}
