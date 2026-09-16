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
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
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

    @Test
    void resetsFailedSchemaEvolutionBeforeReplay() throws Exception {
        FailOnceMetadataApplier metadataApplier = new FailOnceMetadataApplier();
        SchemaCoordinator coordinator =
                new SchemaCoordinator(
                        "regular-schema-coordinator",
                        new MockedOperatorCoordinatorContext(
                                new OperatorID(), Thread.currentThread().getContextClassLoader()),
                        Executors.newSingleThreadExecutor(),
                        metadataApplier,
                        Collections.emptyList(),
                        RouteMode.ALL_MATCH,
                        SchemaChangeBehavior.LENIENT,
                        Duration.ofSeconds(10));
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
