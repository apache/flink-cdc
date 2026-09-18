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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeferredProcessFunctionTest {
    @Test
    void opensOnlyOnFirstInputAndClosesAfterUse() throws Exception {
        TrackingFunction delegate = new TrackingFunction();
        DeferredProcessFunction<String, String> function =
                new DeferredProcessFunction<>(delegate, String.class);
        function.setRuntimeContext(unusedRuntimeContext());
        function.open(DefaultOpenContext.INSTANCE);
        assertThat(delegate.opens).isZero();
        function.processElement("first", null, null);
        function.processElement("second", null, null);
        assertThat(delegate.opens).isEqualTo(1);
        assertThat(delegate.inputs).isEqualTo(2);
        function.close();
        assertThat(delegate.closes).isEqualTo(1);
    }

    @Test
    void neverOpensResourcesForMissingTargetsAndClosesPartialInitialization() throws Exception {
        TrackingFunction delegate = new TrackingFunction();
        DeferredProcessFunction<String, String> function =
                new DeferredProcessFunction<>(delegate, String.class);
        function.close();
        assertThat(delegate.closes).isZero();
        delegate.failOpen = true;
        function.setRuntimeContext(unusedRuntimeContext());
        assertThatThrownBy(() -> function.processElement("first", null, null))
                .hasMessage("cannot initialize table");
        function.close();
        assertThat(delegate.closes).isEqualTo(1);
        assertThat(delegate.inputs).isZero();
    }

    private static RuntimeContext unusedRuntimeContext() {
        return (RuntimeContext)
                java.lang.reflect.Proxy.newProxyInstance(
                        RuntimeContext.class.getClassLoader(),
                        new Class<?>[] {RuntimeContext.class},
                        (proxy, method, args) -> {
                            throw new AssertionError(
                                    "Unexpected context access " + method.getName());
                        });
    }

    private static final class TrackingFunction extends ProcessFunction<String, String> {
        private int opens;
        private int closes;
        private int inputs;
        private boolean failOpen;

        @Override
        public void open(OpenContext context) {
            opens++;
            if (failOpen) {
                throw new IllegalStateException("cannot initialize table");
            }
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            inputs++;
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
