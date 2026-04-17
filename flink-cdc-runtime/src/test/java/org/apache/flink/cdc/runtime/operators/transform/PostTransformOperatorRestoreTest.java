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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/** Restore-focused regression tests for {@link PostTransformOperator}. */
class PostTransformOperatorRestoreTest {

    private static final TableId TABLE_ID = TableId.tableId("restore_db", "restore_table");

    @Test
    void testRestoreWithUpdatedComputedProjectionStillExpandsCreateTableEvent() throws Exception {
        Schema restoredSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("country", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema expandedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("country", DataTypes.STRING())
                        .physicalColumn("confluent__last_updated", DataTypes.TIMESTAMP_LTZ(3))
                        .primaryKey("id")
                        .build();

        OperatorSubtaskState snapshot;
        PostTransformOperator originalOperator =
                PostTransformOperator.newBuilder()
                        .addTransform(TABLE_ID.identifier(), "*", null)
                        .addTimezone("UTC")
                        .build();
        try (OneInputStreamOperatorTestHarness<Event, Event> originalHarness =
                new OneInputStreamOperatorTestHarness<>(originalOperator)) {
            originalHarness.setup(EventSerializer.INSTANCE);
            originalHarness.open();
            originalOperator.processElement(
                    new StreamRecord<>(new CreateTableEvent(TABLE_ID, restoredSchema)));
            snapshot = originalHarness.snapshot(1L, 1L);
        }

        PostTransformOperator restoredOperator =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TABLE_ID.identifier(),
                                "*, CURRENT_TIMESTAMP AS confluent__last_updated",
                                null)
                        .addTimezone("UTC")
                        .build();
        try (OneInputStreamOperatorTestHarness<Event, Event> restoredHarness =
                new OneInputStreamOperatorTestHarness<>(restoredOperator)) {
            restoredHarness.setup(EventSerializer.INSTANCE);
            restoredHarness.initializeState(snapshot);
            restoredHarness.open();

            restoredOperator.processElement(
                    new StreamRecord<>(new CreateTableEvent(TABLE_ID, restoredSchema)));

            List<Event> outputEvents =
                    restoredHarness.getRecordOutput().stream()
                            .map(StreamRecord::getValue)
                            .collect(Collectors.toList());
            Assertions.assertThat(outputEvents)
                    .containsExactly(new CreateTableEvent(TABLE_ID, expandedSchema));
        }
    }
}
