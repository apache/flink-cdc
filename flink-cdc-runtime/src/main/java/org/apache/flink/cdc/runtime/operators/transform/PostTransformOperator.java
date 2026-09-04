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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.pipeline.DecimalPrecisionMode;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A data process function that performs column filtering, calculated column evaluation & final
 * projection.
 */
public class PostTransformOperator extends AbstractStreamOperatorAdapter<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private final PostTransformProcessor processor;

    public static PostTransformOperatorBuilder newBuilder() {
        return new PostTransformOperatorBuilder();
    }

    PostTransformOperator(
            List<TransformRule> transformRules,
            String timezone,
            DecimalPrecisionMode decimalPrecisionMode,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions,
            Map<String, AiModelClient> modelClients) {
        this.processor =
                new PostTransformProcessor(
                        transformRules, timezone, decimalPrecisionMode, udfFunctions, modelClients);
    }

    @Override
    public void open() throws Exception {
        super.open();
        processor.open();
    }

    @Override
    public void close() throws Exception {
        try {
            processor.close();
        } finally {
            super.close();
        }
    }

    @Override
    public void processElement(StreamRecord<Event> element) {
        Event event = element.getValue();
        try {
            Optional<Event> result = processor.process(event);
            if (result.isPresent()) {
                if (result.get() == event) {
                    output.collect(element);
                } else {
                    output.collect(new StreamRecord<>(result.get()));
                }
            }
        } catch (Exception e) {
            throw processor.wrapTransformException("post-transform", event, e);
        }
    }
}
