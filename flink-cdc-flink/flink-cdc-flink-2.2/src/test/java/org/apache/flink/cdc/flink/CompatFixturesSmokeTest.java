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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.cdc.runtime.compat.MockStreamingRuntimeContextAdapter;
import org.apache.flink.cdc.runtime.compat.OperatorStateStoreAdapter;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the flink-cdc-flink2-common test fixtures from this thin module. */
class CompatFixturesSmokeTest {

    @Test
    void createsMockStreamingRuntimeContextWithSubtaskIdentity() {
        MockStreamingRuntimeContext context = MockStreamingRuntimeContextAdapter.create(true, 4, 0);

        assertThat(context.getTaskInfo().getNumberOfParallelSubtasks()).isEqualTo(4);
        assertThat(context.getTaskInfo().getIndexOfThisSubtask()).isEqualTo(0);
    }

    @Test
    void v2StateStoreMethodsThrowUnsupportedByDefault() {
        OperatorStateStoreAdapter store =
                new OperatorStateStoreAdapter() {

                    @Override
                    public <K, V> BroadcastState<K, V> getBroadcastState(
                            MapStateDescriptor<K, V> stateDescriptor) {
                        return null;
                    }

                    @Override
                    public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
                        return null;
                    }

                    @Override
                    public <S> ListState<S> getUnionListState(
                            ListStateDescriptor<S> stateDescriptor) {
                        return null;
                    }

                    @Override
                    public Set<String> getRegisteredStateNames() {
                        return Set.of();
                    }

                    @Override
                    public Set<String> getRegisteredBroadcastStateNames() {
                        return Set.of();
                    }
                };
        org.apache.flink.api.common.state.v2.MapStateDescriptor<String, String> broadcastDesc =
                new org.apache.flink.api.common.state.v2.MapStateDescriptor<>(
                        "broadcast", String.class, String.class);
        org.apache.flink.api.common.state.v2.ListStateDescriptor<String> listDesc =
                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>(
                        "list", String.class);
        org.apache.flink.api.common.state.v2.ListStateDescriptor<String> unionDesc =
                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>(
                        "union", String.class);

        assertThatThrownBy(() -> store.getBroadcastState(broadcastDesc))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getListState(listDesc))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getUnionListState(unionDesc))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
