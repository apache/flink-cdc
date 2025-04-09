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

package org.apache.flink.cdc.runtime.operators.sink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.operators.sink.exception.SinkWrapperException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An operator that processes records to be written into a {@link SinkFunction} in batch mode.
 *
 * <p>The operator is a proxy of {@link StreamSink} in Flink.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 */
@Internal
public class BatchDataSinkFunctionOperator extends StreamSink<Event> {

    public BatchDataSinkFunctionOperator(SinkFunction<Event> userFunction) {
        super(userFunction);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        try {
            super.processElement(element);
        } catch (Exception e) {
            throw new SinkWrapperException(event, e);
        }
    }
}
