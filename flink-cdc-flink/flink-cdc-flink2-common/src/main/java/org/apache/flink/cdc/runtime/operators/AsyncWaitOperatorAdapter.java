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

package org.apache.flink.cdc.runtime.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;

/** Flink 2.2 adapter for an ordered async operator with state-consistent checkpoints. */
@Internal
public class AsyncWaitOperatorAdapter<IN, OUT> extends AsyncWaitOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    public AsyncWaitOperatorAdapter(
            StreamOperatorParameters<OUT> parameters,
            AsyncFunction<IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {
        super(
                parameters,
                asyncFunction,
                timeout,
                capacity,
                AsyncDataStream.OutputMode.ORDERED,
                AsyncRetryStrategies.NO_RETRY_STRATEGY,
                processingTimeService,
                mailboxExecutor);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // The async function owns schema state, so it must not advance beyond records retained in
        // AsyncWaitOperator's recovery queue.
        endInput();
        super.prepareSnapshotPreBarrier(checkpointId);
    }
}
