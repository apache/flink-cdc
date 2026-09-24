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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.unwrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the regular {@link SchemaCoordinator}. */
class SchemaCoordinatorTest {

    private static final TableId TABLE_ID = TableId.parse("db.regular_recovery");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT().notNull()).build();
    private static final AddColumnEvent ADD_EXTRA_V2 =
            new AddColumnEvent(
                    TABLE_ID,
                    Collections.singletonList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("extra_v2", DataTypes.STRING()),
                                    AddColumnEvent.ColumnPosition.LAST,
                                    null)));

    private static final Schema FULL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("age", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    private static final Schema AGE_DROPPED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema SCORE_ADDED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("score", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    @Test
    void resetsFailedSchemaEvolutionBeforeReplay() throws Exception {
        FailOnceMetadataApplier metadataApplier = new FailOnceMetadataApplier();
        SchemaCoordinator coordinator =
                newCoordinator(metadataApplier, SchemaChangeBehavior.LENIENT);
        coordinator.start();

        try {
            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            byte[] checkpoint = checkpoint(coordinator);

            CompletableFuture<CoordinationResponse> failedRequest =
                    submitSchemaChange(coordinator, ADD_EXTRA_V2);
            assertThat(metadataApplier.failedAdd.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> failedRequest.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);

            coordinator.resetToCheckpoint(1L, checkpoint);

            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            SchemaChangeResponse response = requestSchemaChange(coordinator, ADD_EXTRA_V2);

            assertThat(response.getAppliedSchemaChangeEvents()).containsExactly(ADD_EXTRA_V2);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void reAddedTableWithDroppedColumnRecreatesDownstreamTable() throws Exception {
        // Run 1: the table is captured with FULL_SCHEMA, then the job stops with a savepoint.
        byte[] savepoint = runAndCheckpoint(SchemaChangeBehavior.EVOLVE, createEvent(FULL_SCHEMA));

        // Run 2: the table was removed from the pipeline (and the downstream table was dropped),
        // then re-added after "age" got dropped upstream. The source re-snapshots it and emits a
        // CreateTableEvent carrying the latest schema.
        CollectingMetadataApplier applier = new CollectingMetadataApplier(null);
        SchemaCoordinator coordinator = newCoordinator(applier, SchemaChangeBehavior.EVOLVE);
        coordinator.start();
        try {
            coordinator.resetToCheckpoint(1L, savepoint);

            SchemaChangeResponse response =
                    requestSchemaChange(coordinator, createEvent(AGE_DROPPED_SCHEMA));

            // The stale state must not swallow the event: the downstream table is re-created
            // and aligned with the incoming schema.
            assertThat(applier.getSchemaChangeEvents())
                    .containsExactly(
                            createEvent(AGE_DROPPED_SCHEMA),
                            new DropColumnEvent(TABLE_ID, Collections.singletonList("age")));
            // Downstream only needs the resulting CreateTableEvent to refresh its schema view.
            assertThat(response.getAppliedSchemaChangeEvents())
                    .containsExactly(createEvent(AGE_DROPPED_SCHEMA));
            assertThat(latestEvolvedSchema(coordinator)).isEqualTo(AGE_DROPPED_SCHEMA);
            assertThat(latestOriginalSchema(coordinator)).isEqualTo(AGE_DROPPED_SCHEMA);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void reAddedTableWithAddedColumnAlignsDownstreamTable() throws Exception {
        byte[] savepoint = runAndCheckpoint(SchemaChangeBehavior.EVOLVE, createEvent(FULL_SCHEMA));

        CollectingMetadataApplier applier = new CollectingMetadataApplier(null);
        SchemaCoordinator coordinator = newCoordinator(applier, SchemaChangeBehavior.EVOLVE);
        coordinator.start();
        try {
            coordinator.resetToCheckpoint(1L, savepoint);

            SchemaChangeResponse response =
                    requestSchemaChange(coordinator, createEvent(SCORE_ADDED_SCHEMA));

            assertThat(applier.getSchemaChangeEvents())
                    .containsExactly(
                            createEvent(SCORE_ADDED_SCHEMA),
                            new AddColumnEvent(
                                    TABLE_ID,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    SCORE_ADDED_SCHEMA.getColumn("score").get(),
                                                    AddColumnEvent.ColumnPosition.AFTER,
                                                    "age"))));
            assertThat(response.getAppliedSchemaChangeEvents())
                    .containsExactly(createEvent(SCORE_ADDED_SCHEMA));
            assertThat(latestEvolvedSchema(coordinator)).isEqualTo(SCORE_ADDED_SCHEMA);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void lenientBehaviorKeepsDroppedColumnOnReAddedTable() throws Exception {
        byte[] savepoint = runAndCheckpoint(SchemaChangeBehavior.LENIENT, createEvent(FULL_SCHEMA));

        CollectingMetadataApplier applier = new CollectingMetadataApplier(null);
        SchemaCoordinator coordinator = newCoordinator(applier, SchemaChangeBehavior.LENIENT);
        coordinator.start();
        try {
            coordinator.resetToCheckpoint(1L, savepoint);

            SchemaChangeResponse response =
                    requestSchemaChange(coordinator, createEvent(AGE_DROPPED_SCHEMA));

            // LENIENT never narrows the downstream schema: the downstream table is re-created
            // (if absent) with the widened schema keeping "age".
            assertThat(applier.getSchemaChangeEvents()).containsExactly(createEvent(FULL_SCHEMA));
            assertThat(response.getAppliedSchemaChangeEvents())
                    .containsExactly(createEvent(FULL_SCHEMA));
            assertThat(latestEvolvedSchema(coordinator)).isEqualTo(FULL_SCHEMA);
            // The original schema still tracks the upstream truth.
            assertThat(latestOriginalSchema(coordinator)).isEqualTo(AGE_DROPPED_SCHEMA);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void ignoreBehaviorFreezesEvolvedSchemaOnReAddedTable() throws Exception {
        byte[] savepoint = runAndCheckpoint(SchemaChangeBehavior.IGNORE, createEvent(FULL_SCHEMA));

        CollectingMetadataApplier applier = new CollectingMetadataApplier(null);
        SchemaCoordinator coordinator = newCoordinator(applier, SchemaChangeBehavior.IGNORE);
        coordinator.start();
        try {
            coordinator.resetToCheckpoint(1L, savepoint);

            SchemaChangeResponse response =
                    requestSchemaChange(coordinator, createEvent(AGE_DROPPED_SCHEMA));

            // IGNORE freezes the downstream schema, but the (possibly dropped) downstream
            // table still gets re-created with the frozen schema.
            assertThat(applier.getSchemaChangeEvents()).containsExactly(createEvent(FULL_SCHEMA));
            assertThat(response.getAppliedSchemaChangeEvents())
                    .containsExactly(createEvent(FULL_SCHEMA));
            assertThat(latestEvolvedSchema(coordinator)).isEqualTo(FULL_SCHEMA);
            assertThat(latestOriginalSchema(coordinator)).isEqualTo(AGE_DROPPED_SCHEMA);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void duplicateCreateTableEventWithIdenticalSchemaIsSkipped() throws Exception {
        CollectingMetadataApplier applier = new CollectingMetadataApplier(null);
        SchemaCoordinator coordinator = newCoordinator(applier, SchemaChangeBehavior.EVOLVE);
        coordinator.start();
        try {
            SchemaChangeResponse first = requestSchemaChange(coordinator, createEvent(FULL_SCHEMA));
            assertThat(first.getAppliedSchemaChangeEvents())
                    .containsExactly(createEvent(FULL_SCHEMA));

            // Duplicated CreateTableEvents emitted in the snapshot stage must stay no-ops.
            SchemaChangeResponse second =
                    requestSchemaChange(coordinator, createEvent(FULL_SCHEMA));
            assertThat(second.getAppliedSchemaChangeEvents()).isEmpty();
            assertThat(applier.getSchemaChangeEvents()).containsExactly(createEvent(FULL_SCHEMA));
        } finally {
            coordinator.close();
        }
    }

    // ------------------------ Helpers ------------------------

    private static CreateTableEvent createEvent(Schema schema) {
        return new CreateTableEvent(TABLE_ID, schema);
    }

    private static byte[] runAndCheckpoint(
            SchemaChangeBehavior behavior, SchemaChangeEvent... events) throws Exception {
        SchemaCoordinator coordinator =
                newCoordinator(new CollectingMetadataApplier(null), behavior);
        coordinator.start();
        try {
            for (SchemaChangeEvent event : events) {
                requestSchemaChange(coordinator, event);
            }
            return checkpoint(coordinator);
        } finally {
            coordinator.close();
        }
    }

    private static SchemaCoordinator newCoordinator(
            MetadataApplier metadataApplier, SchemaChangeBehavior behavior) {
        return new SchemaCoordinator(
                "regular-schema-coordinator",
                new MockedOperatorCoordinatorContext(
                        new OperatorID(), Thread.currentThread().getContextClassLoader()),
                Executors.newSingleThreadExecutor(),
                metadataApplier,
                Collections.emptyList(),
                RouteMode.ALL_MATCH,
                behavior,
                Duration.ofSeconds(10));
    }

    private static Schema latestEvolvedSchema(SchemaCoordinator coordinator) throws Exception {
        GetEvolvedSchemaResponse response =
                (GetEvolvedSchemaResponse)
                        unwrap(
                                coordinator
                                        .handleCoordinationRequest(
                                                GetEvolvedSchemaRequest.ofLatestSchema(TABLE_ID))
                                        .get(10, TimeUnit.SECONDS));
        return response.getSchema().orElse(null);
    }

    private static Schema latestOriginalSchema(SchemaCoordinator coordinator) throws Exception {
        GetOriginalSchemaResponse response =
                (GetOriginalSchemaResponse)
                        unwrap(
                                coordinator
                                        .handleCoordinationRequest(
                                                GetOriginalSchemaRequest.ofLatestSchema(TABLE_ID))
                                        .get(10, TimeUnit.SECONDS));
        return response.getSchema().orElse(null);
    }

    private static byte[] checkpoint(SchemaCoordinator coordinator) throws Exception {
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        coordinator.checkpointCoordinator(1L, checkpoint);
        return checkpoint.get(10, TimeUnit.SECONDS);
    }

    private static SchemaChangeResponse requestSchemaChange(
            SchemaCoordinator coordinator, SchemaChangeEvent event) throws Exception {
        return unwrap(submitSchemaChange(coordinator, event).get(10, TimeUnit.SECONDS));
    }

    private static CompletableFuture<CoordinationResponse> submitSchemaChange(
            SchemaCoordinator coordinator, SchemaChangeEvent event) {
        CompletableFuture<CoordinationResponse> response =
                coordinator.handleCoordinationRequest(new SchemaChangeRequest(TABLE_ID, event, 0));
        coordinator.handleEventFromOperator(0, 0, new FlushSuccessEvent(0, 0));
        return response;
    }

    private static final class FailOnceMetadataApplier implements MetadataApplier {
        private final AtomicBoolean failNextAdd = new AtomicBoolean(true);
        private final CountDownLatch failedAdd = new CountDownLatch(1);

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            if (schemaChangeEvent instanceof AddColumnEvent
                    && failNextAdd.compareAndSet(true, false)) {
                failedAdd.countDown();
                throw new SchemaEvolveException(schemaChangeEvent, "Injected metadata failure");
            }
        }
    }
}
