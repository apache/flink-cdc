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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;

import com.alibaba.fluss.config.Configuration;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.CLIENT_ID;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.PREFIX_FLUSS_PROPERTIES;

public class FlussDataSinkFactory implements DataSinkFactory {
    public static final String IDENTIFIER = "fluss";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validateExcept(PREFIX_FLUSS_PROPERTIES);

        Map<String, String> flussOptions = new HashMap<>();
        context.getFactoryConfiguration()
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(PREFIX_FLUSS_PROPERTIES)) {
                                flussOptions.put(
                                        key.substring(PREFIX_FLUSS_PROPERTIES.length()), value);
                            } else {
                                flussOptions.put(key, value);
                            }
                        });
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        Configuration config = Configuration.fromMap(flussOptions);
        return new FlussDataSink(config, zoneId);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CLIENT_ID);
        return options;
    }
}
