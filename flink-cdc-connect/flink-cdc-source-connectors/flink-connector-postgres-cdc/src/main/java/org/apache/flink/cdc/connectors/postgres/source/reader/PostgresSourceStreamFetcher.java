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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresSourceRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * Stream fetcher for Postgres. When {@code logicalMessageEnabled} is enabled, lets {@code
 * pg_logical_emit_message} records (op="m") bypass table-based watermark filtering, since logical
 * messages are not bound to a table.
 */
public class PostgresSourceStreamFetcher extends IncrementalSourceStreamFetcher {

    private final boolean logicalMessageEnabled;
    private final List<String> logicalMessagePrefixes;

    public PostgresSourceStreamFetcher(
            FetchTask.Context taskContext,
            int subtaskId,
            boolean logicalMessageEnabled,
            List<String> logicalMessagePrefixes) {
        super(taskContext, subtaskId);
        this.logicalMessageEnabled = logicalMessageEnabled;
        this.logicalMessagePrefixes = logicalMessagePrefixes;
    }

    @Override
    protected boolean shouldEmit(SourceRecord sourceRecord) {
        if (logicalMessageEnabled && PostgresSourceRecordUtils.isLogicalMessage(sourceRecord)) {
            return logicalMessagePrefixes == null
                    || logicalMessagePrefixes.isEmpty()
                    || logicalMessagePrefixes.stream()
                            .anyMatch(
                                    prefix ->
                                            PostgresSourceRecordUtils.getLogicalMessagePrefix(
                                                            sourceRecord)
                                                    .startsWith(prefix));
        }
        return super.shouldEmit(sourceRecord);
    }
}
