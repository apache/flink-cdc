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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.flink.compat.FlinkPipelineBridges;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.flink.compat.FlinkPipelineBridge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Translator used to build {@link DataSource} which will generate a {@link DataStream}. */
@Internal
public class DataSourceTranslator {

    public DataStream<Event> translate(
            SourceDef sourceDef,
            DataSource dataSource,
            StreamExecutionEnvironment env,
            int sourceParallelism,
            OperatorUidGenerator operatorUidGenerator) {
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();
        String sourceName = sourceDef.getName().orElse(generateDefaultSourceName(sourceDef));
        FlinkPipelineBridge bridge = FlinkPipelineBridges.getDefault();
        DataStreamSource<Event> stream =
                bridge.createDataStreamSource(env, eventSourceProvider, sourceName);
        return stream.setParallelism(sourceParallelism)
                .uid(operatorUidGenerator.generateUid("source"));
    }

    public DataSource createDataSource(
            SourceDef sourceDef, Configuration pipelineConfig, StreamExecutionEnvironment env) {
        // Search the data source factory
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sourceDef.getType(), DataSourceFactory.class);
        // Add source JAR to environment
        FactoryDiscoveryUtils.getJarPathByIdentifier(sourceFactory)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));
        FactoryHelper.DefaultContext context =
                new FactoryHelper.DefaultContext(
                        sourceDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader(),
                        env.getConfiguration());
        return sourceFactory.createDataSource(context);
    }

    private String generateDefaultSourceName(SourceDef sourceDef) {
        return String.format("Flink CDC Event Source: %s", sourceDef.getType());
    }
}
