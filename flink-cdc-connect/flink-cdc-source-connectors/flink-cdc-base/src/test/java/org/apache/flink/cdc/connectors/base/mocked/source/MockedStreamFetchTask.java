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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.table.data.GenericRowData;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static org.apache.flink.cdc.connectors.base.mocked.source.MockedSnapshotFetchTask.WATERMARK_TOPIC_NAME;
import static org.apache.flink.cdc.connectors.base.mocked.source.MockedUtils.createWatermarkPartitionMap;
import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

/** A {@link FetchTask} for stream splits. */
public class MockedStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit streamSplit;
    private final @Nullable TableId tableId;
    private boolean taskRunning;

    public MockedStreamFetchTask(StreamSplit streamSplit) {
        this(streamSplit, null);
    }

    public MockedStreamFetchTask(StreamSplit streamSplit, @Nullable TableId tableId) {
        this.streamSplit = streamSplit;
        this.tableId = tableId;
    }

    @Override
    public void execute(Context context) throws Exception {
        try {
            boolean isPartialStreamSplit = streamSplit.splitId().startsWith(STREAM_SPLIT_ID + "-");
            Integer readingSplitIndex = null;
            if (isPartialStreamSplit) {
                readingSplitIndex = streamSplit.splitId().charAt(13) - '0';
            }

            ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
            MockedFetchTaskContext taskContext = (MockedFetchTaskContext) context;
            MockedDialect dialect = (MockedDialect) taskContext.getDataSourceDialect();
            MockedDatabase database = dialect.getMockedDatabase();

            this.taskRunning = true;

            long currentOffsetIndex =
                    ((MockedOffset) streamSplit.getStartingOffset()).getOffsetIndex();
            final long endingOffset =
                    ((MockedOffset) streamSplit.getEndingOffset()).getOffsetIndex();
            outer:
            while (true) {
                Tuple2<Long, List<Tuple3<TableId, Long, GenericRowData>>> result =
                        database.pollStreamValues(
                                tableId, currentOffsetIndex, endingOffset, readingSplitIndex, 4);
                for (Tuple3<TableId, Long, GenericRowData> rec : result.f1) {
                    if (rec.f1 > endingOffset) {
                        break outer;
                    }
                    GenericRowData rowData = rec.f2;
                    long pkValue = rowData.getLong(0);
                    if (!isPartialStreamSplit || Math.floorMod(pkValue, 4) == readingSplitIndex) {
                        changeEventQueue.enqueue(
                                new DataChangeEvent(
                                        new SourceRecord(
                                                createWatermarkPartitionMap(rec.f0.identifier()),
                                                new MockedOffset(rec.f1).getOffset(),
                                                rec.f0.identifier(),
                                                MockedUtils.KEY_SCHEMA,
                                                MockedUtils.createKeySchema(rowData.getLong(0)),
                                                MockedUtils.VALUE_SCHEMA,
                                                MockedUtils.createValueSchema(
                                                        rec.f1,
                                                        rowData.getRowKind(),
                                                        rowData.getLong(0),
                                                        rowData.getString(1)))));
                    }
                }
                currentOffsetIndex = result.f0;
                Thread.sleep(1000);
            }
            SourceRecord endWatermark =
                    WatermarkEvent.create(
                            createWatermarkPartitionMap(tableId.identifier()),
                            WATERMARK_TOPIC_NAME,
                            streamSplit.splitId(),
                            WatermarkKind.END,
                            new MockedOffset(currentOffsetIndex));

            changeEventQueue.enqueue(new DataChangeEvent(endWatermark));
        } finally {
            taskRunning = false;
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return streamSplit;
    }

    @Override
    public void close() {
        taskRunning = false;
    }
}
