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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.runtime.partitioning.EventPartitioner;
import com.ververica.cdc.runtime.partitioning.PartitioningEventKeySelector;
import com.ververica.cdc.runtime.partitioning.PostPartitionProcessor;
import com.ververica.cdc.runtime.partitioning.PrePartitionOperator;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
import com.ververica.cdc.runtime.typeutils.PartitioningEventTypeInfo;

/** Translator for building partitioning related transformations. */
@Internal
public class PartitioningTranslator {

    public DataStream<Event> translate(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            OperatorID schemaOperatorID) {
        return input.transform(
                        "PrePartition",
                        new PartitioningEventTypeInfo(),
                        new PrePartitionOperator(schemaOperatorID, downstreamParallelism))
                .setParallelism(upstreamParallelism)
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                .map(new PostPartitionProcessor(), new EventTypeInfo())
                .name("PostPartition");
    }
}
