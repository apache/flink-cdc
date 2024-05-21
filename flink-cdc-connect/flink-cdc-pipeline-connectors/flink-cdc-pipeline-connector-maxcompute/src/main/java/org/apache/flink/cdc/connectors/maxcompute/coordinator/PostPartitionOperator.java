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

package org.apache.flink.cdc.connectors.maxcompute.coordinator;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.Deque;

/** operator after PartitionOperator to aggregate multi flush event to one. */
public class PostPartitionOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<PartitioningEvent, Event>, BoundedOneInput {

    private final int parallelism;

    private transient Deque<Event> eventQueue;
    private transient int flushEventCount;

    public PostPartitionOperator(int parallelism) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.parallelism = parallelism;
    }

    @Override
    public void open() throws Exception {
        flushEventCount = 0;
        eventQueue = new ArrayDeque<>();
    }

    @Override
    public void processElement(StreamRecord<PartitioningEvent> element) throws Exception {
        Event event = element.getValue().getPayload();
        if (event instanceof FlushEvent) {
            flushEventCount++;
            if (flushEventCount == parallelism) {
                flushEventCount = 0;
                while (!eventQueue.isEmpty()) {
                    output.collect(new StreamRecord<>(eventQueue.poll()));
                }
                output.collect(new StreamRecord<>(event));
            }
        } else {
            eventQueue.offer(event);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        while (!eventQueue.isEmpty()) {
            output.collect(new StreamRecord<>(eventQueue.poll()));
        }
    }

    @Override
    public void endInput() throws Exception {
        while (!eventQueue.isEmpty()) {
            output.collect(new StreamRecord<>(eventQueue.poll()));
        }
    }
}
