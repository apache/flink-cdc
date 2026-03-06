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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;

/**
 * Factory for {@link BatchRegularPrePartitionOperator} with chaining strategy set for Flink 2.x
 * compatibility.
 */
@Internal
public class BatchRegularPrePartitionOperatorFactory
        extends SimpleOperatorFactory<PartitioningEvent>
        implements OneInputStreamOperatorFactory<Event, PartitioningEvent> {

    public BatchRegularPrePartitionOperatorFactory(
            int downstreamParallelism, HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        super(new BatchRegularPrePartitionOperator(downstreamParallelism, hashFunctionProvider));
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }
}
