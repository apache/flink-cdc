/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.paimon.flink.sink.MultiTableCommittable;

import java.util.ArrayList;
import java.util.List;

/** An Operator to add checkpointId to MultiTableCommittable and generate CommittableSummary. */
public class PreCommitOperator
        extends AbstractStreamOperator<CommittableMessage<MultiTableCommittable>>
        implements OneInputStreamOperator<
                CommittableMessage<MultiTableCommittable>,
                CommittableMessage<MultiTableCommittable>> {

    /** store a list of MultiTableCommittable in one checkpoint. */
    private final List<MultiTableCommittable> results;

    public PreCommitOperator() {
        results = new ArrayList<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<MultiTableCommittable>> element) {
        if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
            results.add(
                    ((CommittableWithLineage<MultiTableCommittable>) element.getValue())
                            .getCommittable());
        }
    }

    @Override
    public void finish() {
        prepareSnapshotPreBarrier(Long.MAX_VALUE);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        // CommittableSummary should be sent before all CommittableWithLineage.
        CommittableMessage<MultiTableCommittable> summary =
                new CommittableSummary<>(
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        checkpointId,
                        results.size(),
                        results.size(),
                        0);
        output.collect(new StreamRecord<>(summary));

        results.forEach(
                committable -> {
                    // update the right checkpointId for MultiTableCommittable
                    MultiTableCommittable committableWithCheckPointId =
                            new MultiTableCommittable(
                                    committable.getDatabase(),
                                    committable.getTable(),
                                    checkpointId,
                                    committable.kind(),
                                    committable.wrappedCommittable());
                    CommittableMessage<MultiTableCommittable> message =
                            new CommittableWithLineage<>(
                                    committableWithCheckPointId,
                                    checkpointId,
                                    getRuntimeContext().getIndexOfThisSubtask());
                    output.collect(new StreamRecord<>(message));
                });
        results.clear();
    }
}
