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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Align {@link FlushEvent}s broadcasted by {@link BucketAssignOperator}. */
public class FlushEventAlignmentOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private transient int totalTasksNumber;

    /**
     * Key: subtask id of {@link SchemaOperator}, Value: subtask ids of {@link
     * BucketAssignOperator}.
     */
    private transient Map<Integer, Set<Integer>> sourceTaskIdToAssignBucketSubTaskIds;

    private transient int currentSubTaskId;

    public FlushEventAlignmentOperator() {
        // It's necessary to avoid unpredictable outcomes of Event shuffling.
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.totalTasksNumber = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.currentSubTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        sourceTaskIdToAssignBucketSubTaskIds = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) {
        Event event = streamRecord.getValue();
        if (event instanceof BucketWrapperFlushEvent) {
            BucketWrapperFlushEvent bucketWrapperFlushEvent = (BucketWrapperFlushEvent) event;
            int sourceSubTaskId = bucketWrapperFlushEvent.getSourceSubTaskId();
            Set<Integer> subTaskIds =
                    sourceTaskIdToAssignBucketSubTaskIds.getOrDefault(
                            sourceSubTaskId, new HashSet<>());
            int subtaskId = bucketWrapperFlushEvent.getBucketAssignTaskId();
            subTaskIds.add(subtaskId);
            if (subTaskIds.size() == totalTasksNumber) {
                LOG.info("{} send FlushEvent of {}", currentSubTaskId, sourceSubTaskId);
                output.collect(
                        new StreamRecord<>(
                                new FlushEvent(
                                        sourceSubTaskId,
                                        bucketWrapperFlushEvent.getTableIds(),
                                        bucketWrapperFlushEvent.getSchemaChangeEventType())));
                sourceTaskIdToAssignBucketSubTaskIds.remove(sourceSubTaskId);
            } else {
                LOG.info(
                        "{} collect FlushEvent of {} with subtask {}",
                        currentSubTaskId,
                        sourceSubTaskId,
                        +subtaskId);
                sourceTaskIdToAssignBucketSubTaskIds.put(sourceSubTaskId, subTaskIds);
            }
        } else {
            output.collect(streamRecord);
        }
    }
}
