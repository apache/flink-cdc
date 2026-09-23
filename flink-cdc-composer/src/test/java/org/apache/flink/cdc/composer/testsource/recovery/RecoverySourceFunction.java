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

package org.apache.flink.cdc.composer.testsource.recovery;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.source.RuntimeContextAdapter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Emits an add-column event only after a completed checkpoint and restores its event cursor. */
public class RecoverySourceFunction extends RichParallelSourceFunction<Event>
        implements CheckpointedFunction, CheckpointListener {

    private static final int ADD_FIRST_COLUMN_POSITION = 2;

    private static final AtomicInteger RUN_ATTEMPTS = new AtomicInteger();
    private static final AtomicInteger FIRST_COLUMN_EMISSIONS = new AtomicInteger();

    private transient ListState<Integer> cursorState;
    private transient Map<Long, Integer> checkpointCursors;
    private volatile boolean running = true;
    private volatile boolean checkpointCompleted;
    private int cursor;
    private boolean restored;

    public static void reset() {
        RUN_ATTEMPTS.set(0);
        FIRST_COLUMN_EMISSIONS.set(0);
    }

    public static int getRunAttempts() {
        return RUN_ATTEMPTS.get();
    }

    public static int getFirstColumnEmissions() {
        return FIRST_COLUMN_EMISSIONS.get();
    }

    @Override
    public void run(SourceFunction.SourceContext<Event> context) throws Exception {
        if (RuntimeContextAdapter.getIndexOfThisSubtask(
                        (StreamingRuntimeContext) getRuntimeContext())
                != 0) {
            return;
        }
        RUN_ATTEMPTS.incrementAndGet();
        List<Event> events = events();
        if (restored) {
            synchronized (context.getCheckpointLock()) {
                context.collect(events.get(0));
            }
        }
        while (running && cursor < events.size()) {
            if (cursor == ADD_FIRST_COLUMN_POSITION && !checkpointCompleted) {
                Thread.sleep(10L);
                continue;
            }
            if (cursor == ADD_FIRST_COLUMN_POSITION) {
                synchronized (context.getCheckpointLock()) {
                    FIRST_COLUMN_EMISSIONS.incrementAndGet();
                    context.collect(events.get(cursor));
                    if (restored) {
                        cursor++;
                    }
                }
                // Preserve cursor 2 until the first attempt is cancelled, so it must replay C.
                while (running && !restored) {
                    Thread.sleep(10L);
                }
                continue;
            }
            synchronized (context.getCheckpointLock()) {
                context.collect(events.get(cursor));
                cursor++;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        cursorState.update(Collections.singletonList(cursor));
        checkpointCursors.put(context.getCheckpointId(), cursor);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        cursorState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("event-cursor", Types.INT));
        checkpointCursors = new HashMap<>();
        for (Integer restoredCursor : cursorState.get()) {
            cursor = restoredCursor;
        }
        restored = context.isRestored();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Integer checkpointCursor = checkpointCursors.remove(checkpointId);
        if (checkpointCursor != null && checkpointCursor == ADD_FIRST_COLUMN_POSITION) {
            checkpointCompleted = true;
        }
    }

    private static List<Event> events() {
        TableId tableId = TableId.tableId("recovery", "schema", "orders");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator twoColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()});
        BinaryRecordDataGenerator threeColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                        });
        BinaryRecordDataGenerator fourColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        });
        return Arrays.asList(
                new CreateTableEvent(tableId, initialSchema),
                DataChangeEvent.insertEvent(
                        tableId,
                        twoColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("before")
                                })),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("extra_v1", DataTypes.STRING())))),
                DataChangeEvent.updateEvent(
                        tableId,
                        threeColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("before"),
                                    null
                                }),
                        threeColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1")
                                })),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("extra_v2", DataTypes.STRING())))),
                DataChangeEvent.updateEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1"),
                                    null
                                }),
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("deleted"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("deleted"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })));
    }
}
