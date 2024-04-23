/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.SessionManageCoordinatedOperatorFactory;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeExecutionOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEventKeySelector;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.IOException;

/** A {@link Sink} of {@link Event} to MaxCompute. */
public class MaxComputeEventSink implements Sink<Event>, WithPreWriteTopology<Event> {
    private static final long serialVersionUID = 1L;
    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final MaxComputeExecutionOptions executionOptions;

    public MaxComputeEventSink(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            MaxComputeExecutionOptions executionOptions) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.executionOptions = executionOptions;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> inputDataStream) {
        SingleOutputStreamOperator<Event> withSession =
                inputDataStream.transform(
                        "SessionManageOperator",
                        new EventTypeInfo(),
                        new SessionManageCoordinatedOperatorFactory(
                                options, writeOptions, executionOptions));
        return withSession
                .transform(
                        "PartitionByBucket",
                        new PartitioningEventTypeInfo(),
                        new PartitionOperator(
                                withSession.getParallelism(), options.getBucketSize()))
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                .map(new PostPartitionProcessor(), new EventTypeInfo())
                .name("PartitionByBucket");
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) throws IOException {
        return new MaxComputeEventWriter(options, writeOptions, executionOptions);
    }
}
