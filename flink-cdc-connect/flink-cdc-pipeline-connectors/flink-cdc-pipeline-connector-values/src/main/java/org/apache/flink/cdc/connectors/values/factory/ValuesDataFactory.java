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

package org.apache.flink.cdc.connectors.values.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.RuntimeExecutionMode;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSource;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;

import java.util.HashSet;
import java.util.Set;

/** A dummy {@link Factory} to create {@link ValuesDataSource} and {@link ValuesDataSink}. */
@Internal
public class ValuesDataFactory implements DataSourceFactory, DataSinkFactory {

    public static final String IDENTIFIER = "values";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();
        verifyRuntimeMode(context);
        ValuesDataSourceHelper.EventSetId eventType =
                context.getFactoryConfiguration().get(ValuesDataSourceOptions.EVENT_SET_ID);
        int failAtPos =
                context.getFactoryConfiguration()
                        .get(ValuesDataSourceOptions.FAILURE_INJECTION_INDEX);
        return new ValuesDataSource(eventType, failAtPos);
    }

    private void verifyRuntimeMode(Context context) {
        Configuration pipelineConfiguration = context.getPipelineConfiguration();
        if (pipelineConfiguration == null
                || !pipelineConfiguration.contains(PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE)
                || !RuntimeExecutionMode.BATCH.equals(
                        pipelineConfiguration.get(
                                PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE))) {
            return;
        }
        if (context.getFactoryConfiguration().contains(ValuesDataSourceOptions.EVENT_SET_ID)) {
            ValuesDataSourceHelper.EventSetId eventType =
                    context.getFactoryConfiguration().get(ValuesDataSourceOptions.EVENT_SET_ID);
            if (eventType.equals(ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS)) {
                final Boolean batchModeEnabled =
                        context.getFactoryConfiguration()
                                .get(ValuesDataSourceOptions.BATCH_MODE_ENABLED);
                if (!batchModeEnabled) {
                    throw new IllegalArgumentException(
                            "Batch mode is only supported for Values Data source with configuration 'execution.runtime-mode = BATCH'.");
                }
            } else if (!eventType.isBatchEvent()) {
                throw new IllegalArgumentException(
                        "Batch mode is only supported for Values Data source with batch events.");
            }
        }
    }

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();
        return new ValuesDataSink(
                context.getFactoryConfiguration().get(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY),
                context.getFactoryConfiguration().get(ValuesDataSinkOptions.PRINT_ENABLED),
                context.getFactoryConfiguration().get(ValuesDataSinkOptions.SINK_API),
                context.getFactoryConfiguration()
                        .get(ValuesDataSinkOptions.ERROR_ON_SCHEMA_CHANGE));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ValuesDataSourceOptions.EVENT_SET_ID);
        options.add(ValuesDataSourceOptions.FAILURE_INJECTION_INDEX);
        options.add(ValuesDataSourceOptions.BATCH_MODE_ENABLED);
        options.add(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY);
        options.add(ValuesDataSinkOptions.PRINT_ENABLED);
        options.add(ValuesDataSinkOptions.SINK_API);
        options.add(ValuesDataSinkOptions.ERROR_ON_SCHEMA_CHANGE);
        return options;
    }
}
