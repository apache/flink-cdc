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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.runtime.partitioning.BatchRegularPrePartitionOperator;
import org.apache.flink.cdc.runtime.partitioning.DistributedPrePartitionOperator;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEventKeySelector;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.cdc.runtime.partitioning.RegularPrePartitionOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Translator used to build {@link RegularPrePartitionOperator} or {@link
 * DistributedPrePartitionOperator}, {@link EventPartitioner} and {@link PostPartitionProcessor}
 * that are responsible for events partition.
 */
@Internal
public class PartitioningTranslator {

    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            OperatorID schemaOperatorID,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider,
            OperatorUidGenerator operatorUidGenerator) {
        return translateRegular(
                input,
                upstreamParallelism,
                downstreamParallelism,
                false,
                schemaOperatorID,
                hashFunctionProvider,
                operatorUidGenerator);
    }

    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider,
            OperatorUidGenerator operatorUidGenerator) {
        SingleOutputStreamOperator<Event> singleOutputStreamOperator =
                input.transform(
                                isBatchMode ? "BatchPrePartition" : "PrePartition",
                                new PartitioningEventTypeInfo(),
                                isBatchMode
                                        ? new BatchRegularPrePartitionOperator(
                                                downstreamParallelism, hashFunctionProvider)
                                        : new RegularPrePartitionOperator(
                                                schemaOperatorID,
                                                downstreamParallelism,
                                                hashFunctionProvider))
                        .uid(operatorUidGenerator.generateUid("pre-partition"))
                        .setParallelism(upstreamParallelism)
                        .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                        .map(new PostPartitionProcessor(), new EventTypeInfo())
                        .name(isBatchMode ? "BatchPostPartition" : "PostPartition")
                        .uid(operatorUidGenerator.generateUid("post-partition"));
        return isBatchMode
                ? singleOutputStreamOperator.setParallelism(downstreamParallelism)
                : singleOutputStreamOperator;
    }

    public DataStream<PartitioningEvent> translateDistributed(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        return input.transform(
                        "Partitioning",
                        new PartitioningEventTypeInfo(),
                        new DistributedPrePartitionOperator(
                                downstreamParallelism, hashFunctionProvider))
                .setParallelism(upstreamParallelism)
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector());
    }
}
