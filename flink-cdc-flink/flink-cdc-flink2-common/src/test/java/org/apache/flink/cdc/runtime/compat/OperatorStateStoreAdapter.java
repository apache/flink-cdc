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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.OperatorStateStore;

/**
 * Compatibility adapter for {@link OperatorStateStore} in Flink 2.2.
 *
 * <p>In Flink 2.x, OperatorStateStore interface has new methods using v2 state descriptors:
 *
 * <ul>
 *   <li>{@code getBroadcastState(MapStateDescriptor<K, V>)} - v2 version
 *   <li>{@code getListState(ListStateDescriptor<S>)} - v2 version
 *   <li>{@code getUnionListState(ListStateDescriptor<S>)} - v2 version
 * </ul>
 *
 * <p>This adapter provides default implementations for these new v2 methods that throw {@link
 * UnsupportedOperationException}, allowing test code to implement only the Flink 1.x methods.
 */
@Internal
public interface OperatorStateStoreAdapter extends OperatorStateStore {

    @Experimental
    default <K, V> BroadcastState<K, V> getBroadcastState(
            org.apache.flink.api.common.state.v2.MapStateDescriptor<K, V> var1) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Experimental
    default <S> org.apache.flink.api.common.state.v2.ListState<S> getListState(
            org.apache.flink.api.common.state.v2.ListStateDescriptor<S> var1) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Experimental
    default <S> org.apache.flink.api.common.state.v2.ListState<S> getUnionListState(
            org.apache.flink.api.common.state.v2.ListStateDescriptor<S> var1) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }
}
