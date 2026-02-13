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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.RuntimeExecutionMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.DataSourceTranslator;
import org.apache.flink.cdc.composer.flink.translator.OperatorUidGenerator;
import org.apache.flink.cdc.composer.flink.translator.PartitioningTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.composer.flink.translator.TransformTranslator;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Composer for translating data pipeline to a Flink DataStream job. */
@Internal
public class FlinkPipelineComposer implements PipelineComposer {

    private static final String SCHEMA_OPERATOR_UID_SUFFIX = "schema-operator";
    private final StreamExecutionEnvironment env;
    private final boolean isBlocking;

    public static FlinkPipelineComposer ofRemoteCluster(
            org.apache.flink.configuration.Configuration flinkConfig, List<Path> additionalJars) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(flinkConfig);
        additionalJars.forEach(
                jarPath -> {
                    try {
                        FlinkEnvironmentUtils.addJar(
                                env,
                                jarPath.makeQualified(jarPath.getFileSystem()).toUri().toURL());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Unable to convert JAR path \"%s\" to URL when adding JAR to Flink environment",
                                        jarPath),
                                e);
                    }
                });
        return new FlinkPipelineComposer(env, false);
    }

    public static FlinkPipelineComposer ofApplicationCluster(StreamExecutionEnvironment env) {
        return new FlinkPipelineComposer(env, false);
    }

    public static FlinkPipelineComposer ofMiniCluster() {
        return new FlinkPipelineComposer(
                StreamExecutionEnvironment.getExecutionEnvironment(), true);
    }

    private FlinkPipelineComposer(StreamExecutionEnvironment env, boolean isBlocking) {
        this.env = env;
        this.isBlocking = isBlocking;
    }

    @Override
    public PipelineExecution compose(PipelineDef pipelineDef) {
        Configuration pipelineDefConfig = pipelineDef.getConfig();

        int parallelism = pipelineDefConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        env.getConfig().setParallelism(parallelism);

        translate(env, pipelineDef);

        // Add framework JARs
        addFrameworkJars();

        return new FlinkPipelineExecution(
                env, pipelineDefConfig.get(PipelineOptions.PIPELINE_NAME), isBlocking);
    }

    private void translate(StreamExecutionEnvironment env, PipelineDef pipelineDef) {
        Configuration pipelineDefConfig = pipelineDef.getConfig();
        int parallelism = pipelineDefConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        SchemaChangeBehavior schemaChangeBehavior =
                pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR);

        boolean isBatchMode =
                RuntimeExecutionMode.BATCH.equals(
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE));
        if (isBatchMode) {
            env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.STREAMING);
        }

        // Validate configuration
        String schemaOperatorUid =
                pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        @Nullable
        String operatorUidPrefix =
                pipelineDefConfig.get(PipelineOptions.PIPELINE_OPERATOR_UID_PREFIX);
        if (!Objects.equals(
                        schemaOperatorUid,
                        PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID.defaultValue())
                && operatorUidPrefix != null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Only one of the %s and %s pipeline options can be set.",
                            PipelineOptions.PIPELINE_OPERATOR_UID_PREFIX.key(),
                            PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID.key()));
        }

        OperatorUidGenerator operatorUidGenerator = new OperatorUidGenerator(operatorUidPrefix);

        if (operatorUidPrefix != null) {
            schemaOperatorUid = operatorUidGenerator.generateUid(SCHEMA_OPERATOR_UID_SUFFIX);
        }

        // Initialize translators
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();
        TransformTranslator transformTranslator = new TransformTranslator();
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        schemaChangeBehavior,
                        schemaOperatorUid,
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();

        // And required constructors
        OperatorIDGenerator schemaOperatorIDGenerator = new OperatorIDGenerator(schemaOperatorUid);
        DataSource dataSource =
                sourceTranslator.createDataSource(pipelineDef.getSource(), pipelineDefConfig, env);
        DataSink dataSink =
                sinkTranslator.createDataSink(pipelineDef.getSink(), pipelineDefConfig, env);

        boolean isParallelMetadataSource = dataSource.isParallelMetadataSource();

        // O ---> Source
        DataStream<Event> stream =
                sourceTranslator.translate(
                        pipelineDef.getSource(),
                        dataSource,
                        env,
                        parallelism,
                        operatorUidGenerator);

        // Source ---> PreTransform
        stream =
                transformTranslator.translatePreTransform(
                        stream,
                        pipelineDef.getTransforms(),
                        pipelineDef.getUdfs(),
                        pipelineDef.getModels(),
                        dataSource.supportedMetadataColumns());

        // PreTransform ---> PostTransform
        stream =
                transformTranslator.translatePostTransform(
                        stream,
                        pipelineDef.getTransforms(),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                        pipelineDef.getUdfs(),
                        pipelineDef.getModels(),
                        dataSource.supportedMetadataColumns(),
                        operatorUidGenerator);

        if (isParallelMetadataSource) {
            // Translate a distributed topology for sources with distributed tables
            // PostTransform -> Partitioning
            DataStream<PartitioningEvent> partitionedStream =
                    partitioningTranslator.translateDistributed(
                            stream,
                            parallelism,
                            parallelism,
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism));

            // Partitioning -> Schema Operator
            stream =
                    schemaOperatorTranslator.translateDistributed(
                            partitionedStream,
                            parallelism,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

        } else {
            // Translate a regular topology for sources without distributed tables
            // PostTransform ---> Schema Operator
            stream =
                    schemaOperatorTranslator.translateRegular(
                            stream,
                            parallelism,
                            isBatchMode,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

            // Schema Operator ---(shuffled)---> Partitioning
            stream =
                    partitioningTranslator.translateRegular(
                            stream,
                            parallelism,
                            parallelism,
                            isBatchMode,
                            schemaOperatorIDGenerator.generate(),
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism),
                            operatorUidGenerator);
        }

        // Schema Operator -> Sink -> X
        sinkTranslator.translate(
                pipelineDef.getSink(),
                stream,
                dataSink,
                isBatchMode,
                schemaOperatorIDGenerator.generate(),
                operatorUidGenerator);
    }

    private void addFrameworkJars() {
        try {
            Set<URI> frameworkJars = new HashSet<>();
            // Common JAR
            // We use the core interface (Event) to search the JAR
            Optional<URL> commonJar = getContainingJar(Event.class);
            if (commonJar.isPresent()) {
                frameworkJars.add(commonJar.get().toURI());
            }
            // Runtime JAR
            // We use the serializer of the core interface (EventSerializer) to search the JAR
            Optional<URL> runtimeJar = getContainingJar(EventSerializer.class);
            if (runtimeJar.isPresent()) {
                frameworkJars.add(runtimeJar.get().toURI());
            }
            for (URI jar : frameworkJars) {
                FlinkEnvironmentUtils.addJar(env, jar.toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to search and add Flink CDC framework JARs", e);
        }
    }

    private Optional<URL> getContainingJar(Class<?> clazz) throws Exception {
        URL container = clazz.getProtectionDomain().getCodeSource().getLocation();
        if (Files.isDirectory(Paths.get(container.toURI()))) {
            return Optional.empty();
        }
        return Optional.of(container);
    }

    @VisibleForTesting
    public StreamExecutionEnvironment getEnv() {
        return env;
    }
}
