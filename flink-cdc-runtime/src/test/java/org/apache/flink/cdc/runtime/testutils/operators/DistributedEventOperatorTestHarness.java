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

package org.apache.flink.cdc.runtime.testutils.operators;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.operators.schema.distributed.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Executors;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.unwrap;

/**
 * Harness for testing customized operators handling {@link Event}s in CDC pipeline.
 *
 * <p>In addition to regular operator context and lifecycle management, this test harness also wraps
 * {@link TestingSchemaRegistryGateway} into the context of tested operator, in order to support
 * testing operators that have interaction with {@link SchemaCoordinator} via {@link
 * SchemaEvolutionClient}.
 *
 * @param <OP> Type of the operator
 * @param <E> Type of the event emitted by the operator
 */
public class DistributedEventOperatorTestHarness<
                OP extends AbstractStreamOperator<E>, E extends Event>
        implements AutoCloseable {
    public static final OperatorID SCHEMA_OPERATOR_ID = new OperatorID(15213L, 15513L);

    public static final OperatorID SINK_OPERATOR_ID = new OperatorID(15214L, 15514L);

    private final OP operator;
    private final int numOutputs;
    private final SchemaCoordinator schemaCoordinator;
    private final TestingSchemaRegistryGateway schemaRegistryGateway;
    private final LinkedList<StreamRecord<E>> outputRecords = new LinkedList<>();
    private final MockedOperatorCoordinatorContext mockedContext;

    public DistributedEventOperatorTestHarness(OP operator, int numOutputs) {
        this(operator, numOutputs, Duration.ofSeconds(3), Duration.ofMinutes(3));
    }

    public DistributedEventOperatorTestHarness(
            OP operator, int numOutputs, Duration applyDuration, Duration rpcTimeout) {
        this.operator = operator;
        this.numOutputs = numOutputs;
        this.mockedContext =
                new MockedOperatorCoordinatorContext(
                        SCHEMA_OPERATOR_ID, Thread.currentThread().getContextClassLoader());
        this.schemaCoordinator =
                new SchemaCoordinator(
                        "SchemaRegistry",
                        mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(applyDuration),
                        new ArrayList<>(),
                        SchemaChangeBehavior.LENIENT,
                        rpcTimeout);
        this.schemaRegistryGateway = new TestingSchemaRegistryGateway(schemaCoordinator);
    }

    public void open() throws Exception {
        schemaCoordinator.start();
        initializeOperator();
        operator.open();
    }

    public LinkedList<StreamRecord<E>> getOutputRecords() {
        return outputRecords;
    }

    public void clearOutputRecords() {
        outputRecords.clear();
    }

    public OP getOperator() {
        return operator;
    }

    public void registerTableSchema(TableId tableId, Schema schema) {
        schemaCoordinator.emplaceOriginalSchema(tableId, 0, schema);
        schemaCoordinator.emplaceEvolvedSchema(tableId, schema);
    }

    public Schema getLatestEvolvedSchema(TableId tableId) throws Exception {
        return ((GetEvolvedSchemaResponse)
                        unwrap(
                                schemaCoordinator
                                        .handleCoordinationRequest(
                                                new GetEvolvedSchemaRequest(
                                                        tableId,
                                                        GetEvolvedSchemaRequest
                                                                .LATEST_SCHEMA_VERSION))
                                        .get()))
                .getSchema()
                .orElse(null);
    }

    public boolean isJobFailed() {
        return mockedContext.isJobFailed();
    }

    public Throwable getJobFailureCause() {
        return mockedContext.getFailureCause();
    }

    @Override
    public void close() throws Exception {
        operator.close();
    }

    // -------------------------------------- Helper functions -------------------------------

    private void initializeOperator() throws Exception {
        operator.setup(
                new MockStreamTask(schemaRegistryGateway),
                new MockStreamConfig(new Configuration(), numOutputs),
                new EventCollectingOutput<>(outputRecords, schemaRegistryGateway));
        schemaRegistryGateway.sendOperatorEventToCoordinator(
                SINK_OPERATOR_ID, new SerializedValue<>(new SinkWriterRegisterEvent(0)));
    }

    // ---------------------------------------- Helper classes ---------------------------------

    private static class EventCollectingOutput<E extends Event> implements Output<StreamRecord<E>> {
        private final LinkedList<StreamRecord<E>> outputRecords;

        private final TestingSchemaRegistryGateway schemaRegistryGateway;

        public EventCollectingOutput(
                LinkedList<StreamRecord<E>> outputRecords,
                TestingSchemaRegistryGateway schemaRegistryGateway) {
            this.outputRecords = outputRecords;
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public void collect(StreamRecord<E> record) {
            outputRecords.add(record);
            Event event = record.getValue();
            if (event instanceof FlushEvent) {
                try {
                    schemaRegistryGateway.sendOperatorEventToCoordinator(
                            SINK_OPERATOR_ID, new SerializedValue<>(new FlushSuccessEvent(0, 0)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void close() {}
    }

    private static class MockStreamTask extends StreamTask<Event, AbstractStreamOperator<Event>> {
        protected MockStreamTask(TestingSchemaRegistryGateway schemaRegistryGateway)
                throws Exception {
            super(new SchemaRegistryCoordinatingEnvironment(schemaRegistryGateway));
        }

        @Override
        protected void init() {}
    }

    private static class SchemaRegistryCoordinatingEnvironment extends DummyEnvironment {
        private final TestingSchemaRegistryGateway schemaRegistryGateway;

        public SchemaRegistryCoordinatingEnvironment(
                TestingSchemaRegistryGateway schemaRegistryGateway) {
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
            return schemaRegistryGateway;
        }
    }
}
