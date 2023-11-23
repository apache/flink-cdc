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

package com.ververica.cdc.connectors.starrocks.factory;

import org.apache.flink.configuration.Configuration;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSink;

import java.util.HashMap;
import java.util.Set;

/** A {@link DataSinkFactory} to create {@link StarRocksDataSink}. */
public class StarRocksDataFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "starrocks";

    @Override
    public DataSink createDataSink(Context context) {
        Configuration flinkConfig = Configuration.fromMap(context.getConfiguration().toMap());
        StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(flinkConfig, new HashMap<>());
        return new StarRocksDataSink(sinkOptions);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        throw new UnsupportedOperationException();
    }
}
