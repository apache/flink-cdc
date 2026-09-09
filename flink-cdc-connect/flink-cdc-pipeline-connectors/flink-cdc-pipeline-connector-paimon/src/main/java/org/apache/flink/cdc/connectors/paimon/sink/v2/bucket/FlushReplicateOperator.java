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

package org.apache.flink.cdc.connectors.paimon.sink.v2.bucket;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Replicates each distributed {@link FlushEvent} to every bucket assigner.
 *
 * <p>The source partition and emitting schema subtask are encoded into an internal alignment key.
 * This keeps concurrent flushes independent even when schema subtasks process broadcasts in
 * different orders.
 */
public class FlushReplicateOperator extends AbstractStreamOperatorAdapter<Tuple2<Integer, Event>>
        implements OneInputStreamOperator<Event, Tuple2<Integer, Event>> {

    private transient int parallelism;
    private transient int subtaskId;

    public FlushReplicateOperator() {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) {
        Event event = streamRecord.getValue();
        if (event instanceof FlushEvent) {
            FlushEvent flushEvent = (FlushEvent) event;
            int alignmentKey =
                    Math.addExact(
                            Math.multiplyExact(flushEvent.getSourceSubTaskId(), parallelism),
                            subtaskId);
            FlushEvent replicatedFlush =
                    new FlushEvent(
                            alignmentKey,
                            flushEvent.getTableIds(),
                            flushEvent.getSchemaChangeEventType());
            for (int target = 0; target < parallelism; target++) {
                Event payload =
                        target == subtaskId
                                ? replicatedFlush
                                : EventSerializer.INSTANCE.copy(replicatedFlush);
                output.collect(new StreamRecord<>(Tuple2.of(target, payload)));
            }
        } else {
            output.collect(new StreamRecord<>(Tuple2.of(subtaskId, event)));
        }
    }

    public static Integer targetSubtask(Tuple2<Integer, Event> tuple) {
        return tuple.f0;
    }

    public static Event unwrap(Tuple2<Integer, Event> tuple) {
        return tuple.f1;
    }
}
