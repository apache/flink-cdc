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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresSourceRecordUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.cdc.common.utils.Preconditions.checkState;

/** Record emitter that recognizes {@link PostgresSchemaRecord} as schema change events. */
public class PostgresSourceRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    protected final PostgresSourceConfig sourceConfig;

    public PostgresSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            PostgresSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                includeSchemaChanges,
                offsetFactory);
        this.sourceConfig = sourceConfig;
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isIncludeEmitLogicalMessage(element)) {
            updateStreamSplitState(splitState, element);
            emitElement(element, output);
            return;
        }
        super.processElement(element, output, splitState);
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        // logical message (which op is 'm') is not a data change record.
        return PostgresSourceRecordUtils.isDataChangeRecord(record);
    }

    @Override
    protected TableChanges getTableChangeRecord(SourceRecord element) throws IOException {
        if (element instanceof PostgresSchemaRecord) {
            PostgresSchemaRecord schemaRecord = (PostgresSchemaRecord) element;
            Table table = schemaRecord.getTable();
            return new TableChanges().create(table);
        } else {
            return super.getTableChangeRecord(element);
        }
    }

    private boolean isIncludeEmitLogicalMessage(SourceRecord record) {
        List<String> prefixes = sourceConfig.getLogicalMessagePrefixes();
        if (prefixes == null
                || prefixes.isEmpty()
                || !PostgresSourceRecordUtils.isLogicalMessage(record)) {
            return false;
        }

        String prefix = PostgresSourceRecordUtils.getLogicalMessagePrefix(record);
        checkState(prefix != null, "message_prefix can not be null for logical_message");

        return prefixes.stream().anyMatch(prefix::startsWith);
    }
}
