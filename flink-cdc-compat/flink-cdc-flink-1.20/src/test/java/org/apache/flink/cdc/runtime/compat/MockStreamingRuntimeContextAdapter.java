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

package org.apache.flink.cdc.runtime.compat;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

/**
 * Compatibility adapter for {@link MockStreamingRuntimeContext} in Flink 1.20.
 *
 * <p>In Flink 1.x, MockStreamingRuntimeContext constructor signature is:
 * MockStreamingRuntimeContext(boolean isCheckpointingEnabled, int numberOfParallelSubtasks, int
 * subtaskIndex)
 *
 * <p>This adapter provides a factory method to create MockStreamingRuntimeContext with the Flink
 * 1.x constructor signature.
 */
@Internal
public class MockStreamingRuntimeContextAdapter {

    /**
     * Creates a MockStreamingRuntimeContext with the standard Flink 1.x constructor signature.
     *
     * @param isCheckpointingEnabled whether checkpointing is enabled
     * @param numberOfParallelSubtasks the total number of parallel subtasks
     * @param subtaskIndex the index of this subtask
     * @return a new MockStreamingRuntimeContext instance
     */
    public static MockStreamingRuntimeContext create(
            boolean isCheckpointingEnabled, int numberOfParallelSubtasks, int subtaskIndex) {
        return new MockStreamingRuntimeContext(
                isCheckpointingEnabled, numberOfParallelSubtasks, subtaskIndex);
    }
}
