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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/** Translator used to build {@link DataSource} which will generate a {@link DataStream}. */
@Internal
public class DataSourceTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceTranslator.class);

    private static final String FLINK_SOURCE_FUNCTION_PROVIDER_CLASS =
            "org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider";
    private static final String SOURCE_FUNCTION_CLASS =
            "org.apache.flink.streaming.api.functions.source.SourceFunction";

    public DataStream<Event> translate(
            SourceDef sourceDef,
            DataSource dataSource,
            StreamExecutionEnvironment env,
            int sourceParallelism,
            OperatorUidGenerator operatorUidGenerator) {
        // Get source provider
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();

        return createDataStreamSource(env, eventSourceProvider, sourceDef)
                .setParallelism(sourceParallelism)
                .uid(operatorUidGenerator.generateUid("source"));
    }

    private DataStreamSource<Event> createDataStreamSource(
            StreamExecutionEnvironment env,
            EventSourceProvider eventSourceProvider,
            SourceDef sourceDef) {
        // Source (New Source API, works with both Flink 1.x and 2.x)
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                    sourceProvider.getSource(),
                    WatermarkStrategy.noWatermarks(),
                    sourceDef.getName().orElse(generateDefaultSourceName(sourceDef)),
                    new EventTypeInfo());
        }

        // SourceFunction (Legacy API, Flink 1.x only)
        // Use reflection since FlinkSourceFunctionProvider and SourceFunction are not available
        // in Flink 2.x builds.
        DataStreamSource<Event> legacyResult =
                tryCreateLegacyDataStreamSource(env, eventSourceProvider, sourceDef);
        if (legacyResult != null) {
            return legacyResult;
        }

        // Unknown provider type
        throw new IllegalStateException(
                String.format(
                        "Unsupported EventSourceProvider type \"%s\"",
                        eventSourceProvider.getClass().getCanonicalName()));
    }

    @SuppressWarnings("unchecked")
    private DataStreamSource<Event> tryCreateLegacyDataStreamSource(
            StreamExecutionEnvironment env,
            EventSourceProvider eventSourceProvider,
            SourceDef sourceDef) {
        try {
            Class<?> sourceFuncProviderClass = Class.forName(FLINK_SOURCE_FUNCTION_PROVIDER_CLASS);
            if (!sourceFuncProviderClass.isInstance(eventSourceProvider)) {
                return null;
            }
            // Get the SourceFunction via reflection
            Object sourceFunction =
                    sourceFuncProviderClass
                            .getMethod("getSourceFunction")
                            .invoke(eventSourceProvider);
            // Call env.addSource(sourceFunction, typeInfo) via reflection
            Class<?> sourceFunctionClass = Class.forName(SOURCE_FUNCTION_CLASS);
            Method addSourceMethod =
                    StreamExecutionEnvironment.class.getMethod(
                            "addSource", sourceFunctionClass, TypeInformation.class);
            DataStreamSource<Event> stream =
                    (DataStreamSource<Event>)
                            addSourceMethod.invoke(env, sourceFunction, new EventTypeInfo());
            sourceDef.getName().ifPresent(stream::name);
            return stream;
        } catch (ClassNotFoundException e) {
            LOG.debug(
                    "FlinkSourceFunctionProvider or SourceFunction not available (likely Flink 2.x), "
                            + "skipping legacy source function path.");
            return null;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create legacy data stream source via reflection", e);
        }
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
