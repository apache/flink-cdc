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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.composer.definition.SourceDef;
import com.ververica.cdc.composer.flink.FlinkEnvironmentUtils;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

/**
 * Translator for building source and generate a {@link
 * org.apache.flink.streaming.api.datastream.DataStream}.
 */
@Internal
public class DataSourceTranslator {

    public DataStreamSource<Event> translate(
            SourceDef sourceDef, StreamExecutionEnvironment env, Configuration pipelineConfig) {
        // Search the data source factory
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sourceDef.getType(), DataSourceFactory.class);

        // Create data source
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                sourceDef.getConfig(),
                                pipelineConfig,
                                Thread.currentThread().getContextClassLoader()));

        // Add source JAR to environment
        FactoryDiscoveryUtils.getJarPathByIdentifier(sourceDef.getType(), DataSourceFactory.class)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // Get source provider
        final int sourceParallelism = pipelineConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            // Source
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                            sourceProvider.getSource(),
                            WatermarkStrategy.noWatermarks(),
                            sourceDef.getName().orElse(generateDefaultSourceName(sourceDef)),
                            new EventTypeInfo())
                    .setParallelism(sourceParallelism);
        } else if (eventSourceProvider instanceof FlinkSourceFunctionProvider) {
            // SourceFunction
            FlinkSourceFunctionProvider sourceFunctionProvider =
                    (FlinkSourceFunctionProvider) eventSourceProvider;
            DataStreamSource<Event> stream =
                    env.addSource(sourceFunctionProvider.getSourceFunction(), new EventTypeInfo())
                            .setParallelism(sourceParallelism);
            if (sourceDef.getName().isPresent()) {
                stream.name(sourceDef.getName().get());
            }
            return stream;
        } else {
            // Unknown provider type
            throw new IllegalStateException(
                    String.format(
                            "Unsupported EventSourceProvider type \"%s\"",
                            eventSourceProvider.getClass().getCanonicalName()));
        }
    }

    private String generateDefaultSourceName(SourceDef sourceDef) {
        return String.format("Flink CDC Event Source: %s", sourceDef.getType());
    }
}
