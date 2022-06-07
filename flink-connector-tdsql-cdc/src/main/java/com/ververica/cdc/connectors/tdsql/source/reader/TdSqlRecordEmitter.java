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

package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.tdsql.source.split.TdSqlSplitState;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * The {@link RecordEmitter} implementation for {@link TdSqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public class TdSqlRecordEmitter<T> implements RecordEmitter<SourceRecord, T, TdSqlSplitState> {
    private final MySqlRecordEmitter<T> mySqlRecordEmitter;

    public TdSqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges) {
        this.mySqlRecordEmitter =
                new MySqlRecordEmitter<>(
                        debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges);
    }

    @Override
    public void emitRecord(SourceRecord element, SourceOutput<T> output, TdSqlSplitState splitState)
            throws Exception {
        this.mySqlRecordEmitter.emitRecord(element, output, splitState.mySqlSplitState());
    }
}
