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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;

/** {@link FetchTask.Context} of mocked source. */
public class MockedFetchTaskContext implements FetchTask.Context {

    private final MockedDialect dialect;
    private final MockedConfig sourceConfig;

    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public MockedFetchTaskContext(MockedDialect dialect, MockedConfig sourceConfig) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(50))
                        .maxBatchSize(10)
                        .maxQueueSize(10)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "mocked",
                                                "mocked-connector",
                                                "mocked-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return TableId.parse(record.topic());
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return Tables.TableFilter.includeAll();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new MockedOffset(((Struct) record.value()).getInt64("timestamp"));
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return !isWatermarkEvent(record);
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        long splitStartOffset = (long) splitStart[0];
        long splitEndOffset = (long) splitEnd[0];
        long currentOffset = ((Struct) record.value()).getInt64("id");
        return splitStartOffset <= currentOffset && currentOffset < splitEndOffset;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {}

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
        return new ArrayList<>(snapshotRecords);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public DataSourceDialect<?> getDataSourceDialect() {
        return dialect;
    }

    @Override
    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
