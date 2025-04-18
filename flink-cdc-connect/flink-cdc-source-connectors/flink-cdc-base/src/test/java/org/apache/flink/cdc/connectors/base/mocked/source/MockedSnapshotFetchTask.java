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
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.table.data.RowData;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.base.mocked.source.MockedUtils.createWatermarkPartitionMap;

/** An {@link AbstractScanFetchTask} for snapshot splits. */
public class MockedSnapshotFetchTask extends AbstractScanFetchTask {

    public static final String WATERMARK_TOPIC_NAME = "__mocked_watermarks";

    public MockedSnapshotFetchTask(SnapshotSplit snapshotSplit) {
        super(snapshotSplit);
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        MockedStreamFetchTask backfillStreamTask =
                new MockedStreamFetchTask(backfillStreamSplit, snapshotSplit.getTableId());
        backfillStreamTask.execute(context);
    }

    @Override
    protected void dispatchLowWaterMarkEvent(
            Context context, SourceSplitBase split, Offset lowWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                snapshotSplit.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));
    }

    @Override
    protected void dispatchHighWaterMarkEvent(
            Context context, SourceSplitBase split, Offset highWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                snapshotSplit.splitId(),
                                WatermarkKind.HIGH,
                                highWatermark)));
    }

    @Override
    protected void dispatchEndWaterMarkEvent(
            Context context, SourceSplitBase split, Offset endWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                snapshotSplit.splitId(),
                                WatermarkKind.END,
                                endWatermark)));
    }

    @Override
    protected void executeDataSnapshot(Context context) throws InterruptedException {
        SnapshotSplit split = getSplit();
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        MockedFetchTaskContext mockedContext = (MockedFetchTaskContext) context;
        MockedDatabase mockedDatabase =
                ((MockedDialect) mockedContext.getDataSourceDialect()).getMockedDatabase();

        // Imitating that it costs a long time to finish snapshot reading

        try {
            Tuple2<Long, List<RowData>> snapshotResult =
                    mockedDatabase.takeSnapshot(
                            split.getTableId(),
                            Optional.ofNullable(split.getSplitStart())
                                    .map(arr -> arr[0])
                                    .map(Long.class::cast)
                                    .orElse(Long.MIN_VALUE),
                            Optional.ofNullable(split.getSplitEnd())
                                    .map(arr -> arr[0])
                                    .map(Long.class::cast)
                                    .orElse(Long.MAX_VALUE));

            Thread.sleep(1000L);

            snapshotResult.f1.forEach(
                    rowData -> {
                        try {
                            changeEventQueue.enqueue(
                                    new DataChangeEvent(
                                            new SourceRecord(
                                                    createWatermarkPartitionMap(
                                                            snapshotSplit
                                                                    .getTableId()
                                                                    .identifier()),
                                                    new MockedOffset(snapshotResult.f0).getOffset(),
                                                    getSplit().getTableId().identifier(),
                                                    MockedUtils.KEY_SCHEMA,
                                                    MockedUtils.createKeySchema(rowData.getLong(0)),
                                                    MockedUtils.VALUE_SCHEMA,
                                                    MockedUtils.createValueSchema(
                                                            null,
                                                            rowData.getRowKind(),
                                                            rowData.getLong(0),
                                                            rowData.getString(1)))));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            taskRunning = false;
            throw e;
        }
    }
}
