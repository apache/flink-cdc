/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.values.factory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.values.sink.ValuesDataSink;
import com.ververica.cdc.connectors.values.sink.ValuesDataSinkOptions;
import com.ververica.cdc.connectors.values.source.ValuesDataSource;
import com.ververica.cdc.connectors.values.source.ValuesDataSourceHelper;
import com.ververica.cdc.connectors.values.source.ValuesDataSourceOptions;

import java.util.HashSet;
import java.util.Set;

/** A dummy {@link Factory} to create {@link ValuesDataSource} and {@link ValuesDataSink}. */
@Internal
public class ValuesDataFactory implements DataSourceFactory, DataSinkFactory {

    public static final String IDENTIFIER = "values";

    @Override
    public DataSource createDataSource(Context context) {
        ValuesDataSourceHelper.SourceEventType eventType =
                context.getConfiguration().get(ValuesDataSourceOptions.SOURCE_EVENT_TYPE);
        int failAtPos =
                context.getConfiguration().get(ValuesDataSourceOptions.FAILURE_INJECTION_INDEX);
        return new ValuesDataSource(eventType, failAtPos);
    }

    @Override
    public DataSink createDataSink(Context context) {
        return new ValuesDataSink(
                context.getConfiguration().get(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY));
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
        options.add(ValuesDataSourceOptions.SOURCE_EVENT_TYPE);
        options.add(ValuesDataSourceOptions.FAILURE_INJECTION_INDEX);
        options.add(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY);
        return options;
    }
}
