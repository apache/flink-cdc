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

package org.apache.flink.cdc.flink;

import org.apache.flink.cdc.runtime.compat.MockStreamingRuntimeContextAdapter;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the flink-cdc-flink1-common test fixtures from this thin module. */
class CompatFixturesSmokeTest {

    @Test
    void createsMockStreamingRuntimeContextWithSubtaskIdentity() {
        MockStreamingRuntimeContext context = MockStreamingRuntimeContextAdapter.create(true, 4, 0);

        assertThat(context.getNumberOfParallelSubtasks()).isEqualTo(4);
        assertThat(context.getIndexOfThisSubtask()).isEqualTo(0);
    }
}
