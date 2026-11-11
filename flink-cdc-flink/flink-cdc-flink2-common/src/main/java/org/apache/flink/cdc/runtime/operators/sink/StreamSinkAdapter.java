/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;

/**
 * Compatibility adapter for the Flink 2.x line: exposes the {@link StreamSink} constructor so main
 * code never references the Flink API surface whose parameter type moves across Flink generations
 * (on Flink 2.x {@code SinkFunction} lives in the {@code legacy} subpackage). The adapter is
 * compiled per Flink line, keeping the descriptor in main-code bytecode stable.
 */
@Internal
public class StreamSinkAdapter<IN> extends StreamSink<IN> {

    public StreamSinkAdapter(SinkFunction<IN> userFunction) {
        super(userFunction);
    }

    /**
     * Finishes the wrapped user function. Routed through the adapter because the declaring
     * interface of {@code finish()} moves to the {@code legacy} subpackage on Flink 2.x, which
     * would fork the {@code invokeinterface} owner in main-code bytecode.
     */
    public void finishUserFunction() throws Exception {
        userFunction.finish();
    }
}
