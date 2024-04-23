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

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeExecutionOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.sink.MaxComputeEventSink;

/** A {@link DataSink} for "MaxCompute" connector. */
public class MaxComputeDataSink implements DataSink {
    private static final long serialVersionUID = 1L;
    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final MaxComputeExecutionOptions executionOptions;

    public MaxComputeDataSink(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            MaxComputeExecutionOptions executionOptions) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.executionOptions = executionOptions;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(
                new MaxComputeEventSink(options, writeOptions, executionOptions));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new MaxComputeMetadataApplier(options);
    }
}
