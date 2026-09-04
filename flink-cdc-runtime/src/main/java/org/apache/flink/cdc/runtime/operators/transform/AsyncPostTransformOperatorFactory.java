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

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.operators.AsyncWaitOperatorAdapter;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;

/** Factory for an ordered async post-transform operator with state-consistent checkpoints. */
@Internal
public class AsyncPostTransformOperatorFactory extends AsyncWaitOperatorFactory<Event, Event> {

    private static final long serialVersionUID = 1L;

    private final AsyncPostTransformFunction asyncFunction;
    private final long timeout;
    private final int capacity;

    public AsyncPostTransformOperatorFactory(
            AsyncPostTransformFunction asyncFunction, long timeout, int capacity) {
        super(asyncFunction, timeout, capacity, AsyncDataStream.OutputMode.ORDERED);
        this.asyncFunction = asyncFunction;
        this.timeout = timeout;
        this.capacity = capacity;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Event>> T createStreamOperator(
            StreamOperatorParameters<Event> parameters) {
        return (T)
                new AsyncWaitOperatorAdapter<>(
                        parameters,
                        asyncFunction,
                        timeout,
                        capacity,
                        processingTimeService,
                        getMailboxExecutor());
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AsyncWaitOperatorAdapter.class;
    }
}
