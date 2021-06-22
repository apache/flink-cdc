/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.ververica.cdc.debezium.internal;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DelegatedChangeConsumer<S, T> implements DebeziumEngine.ChangeConsumer<ChangeEvent<S, T>> {

    private final DebeziumEngine.ChangeConsumer<ChangeEvent<S, T>> changeConsumer;

    private final List<Interceptor> interceptors;

    public DelegatedChangeConsumer(DebeziumEngine.ChangeConsumer<ChangeEvent<S, T>> changeConsumer, Interceptor... interceptors) {
        this.changeConsumer = changeConsumer;
        this.interceptors = Stream.of(interceptors).collect(Collectors.toList());
    }

    @Override
    public void handleBatch(List<ChangeEvent<S, T>> list, DebeziumEngine.RecordCommitter<ChangeEvent<S, T>> recordCommitter) throws InterruptedException {
        Interceptor.Context context = new SimpleContext();
        context.put("data", list);
        context.put("ts", System.currentTimeMillis());
        for (Interceptor interceptor : interceptors) {
            interceptor.pre(context);
        }
        changeConsumer.handleBatch(list, recordCommitter);
        for (Interceptor interceptor : interceptors) {
            interceptor.post(context);
        }
    }

    static class SimpleContext implements Interceptor.Context {

        private final Map<String, Object> params = new HashMap<>();
        @Override
        public void put(String key, Object value) {
            params.put(key, value);
        }

        @Override
        public int size() {
            return params.size();
        }

        @Override
        public <T> T get(String key, Class<T> type) {
            return (T) get(key);
        }

        @Override
        public Object get(String key) {
            return params.get(key);
        }

        @Override
        public Iterator<String> iterator() {
            return params.keySet().iterator();
        }
    }
}
