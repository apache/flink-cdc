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
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.iceberg.flink.TableLoader;

/** Opens a stateless Iceberg function only after its first input, when its table exists. */
final class DeferredProcessFunction<I, O> extends ProcessFunction<I, O>
        implements ResultTypeQueryable<O> {
    private static final long serialVersionUID = 1L;
    private final ProcessFunction<I, O> delegate;
    private transient boolean opened;
    private transient boolean openAttempted;
    private final Class<O> outputType;
    private final TableLoader ownedLoader;

    DeferredProcessFunction(ProcessFunction<I, O> delegate, Class<O> outputType) {
        this(delegate, outputType, null);
    }

    DeferredProcessFunction(
            ProcessFunction<I, O> delegate, Class<O> outputType, TableLoader ownedLoader) {
        this.delegate = delegate;
        this.outputType = outputType;
        this.ownedLoader = ownedLoader;
    }

    @Override
    public TypeInformation<O> getProducedType() {
        return TypeInformation.of(outputType);
    }

    ProcessFunction<I, O> delegate() {
        return delegate;
    }

    @Override
    public void processElement(I value, Context context, Collector<O> out) throws Exception {
        if (!opened) {
            delegate.setRuntimeContext(getRuntimeContext());
            openAttempted = true;
            FunctionUtils.openFunction(delegate, DefaultOpenContext.INSTANCE);
            opened = true;
        }
        delegate.processElement(value, context, out);
    }

    @Override
    public void close() throws Exception {
        try (TableLoader ignored = ownedLoader) {
            if (openAttempted) {
                delegate.close();
            }
        }
    }
}
