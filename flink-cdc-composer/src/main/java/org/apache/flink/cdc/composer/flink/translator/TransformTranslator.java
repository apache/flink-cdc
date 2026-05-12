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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.AiModelClientFactory;
import org.apache.flink.cdc.common.model.ModelContext;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperatorBuilder;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperatorBuilder;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
public class TransformTranslator {

    public DataStream<Event> translatePreTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        if (transforms.isEmpty()) {
            return input;
        }
        return input.transform(
                "Transform:Schema",
                new EventTypeInfo(),
                generatePreTransform(transforms, udfFunctions, supportedMetadataColumns));
    }

    private PreTransformOperator generatePreTransform(
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            SupportedMetadataColumn[] supportedMetadataColumns) {

        PreTransformOperatorBuilder preTransformFunctionBuilder = PreTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            preTransformFunctionBuilder.addTransform(
                    transform.getSourceTable(),
                    transform.getProjection(),
                    transform.getFilter(),
                    transform.getPrimaryKeys(),
                    transform.getPartitionKeys(),
                    transform.getTableOptions(),
                    transform.getTableOptionsDelimiter(),
                    transform.getPostTransformConverter(),
                    supportedMetadataColumns);
        }

        preTransformFunctionBuilder.addUdfFunctions(
                udfFunctions.stream().map(this::udfDefToUDFTuple).collect(Collectors.toList()));
        return preTransformFunctionBuilder.build();
    }

    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            String timezone,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns,
            OperatorUidGenerator operatorUidGenerator) {
        if (transforms.isEmpty()) {
            return input;
        }

        PostTransformOperatorBuilder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            postTransformFunctionBuilder.addTransform(
                    transform.getSourceTable(),
                    transform.getProjection(),
                    transform.getFilter(),
                    transform.getPrimaryKeys(),
                    transform.getPartitionKeys(),
                    transform.getTableOptions(),
                    transform.getTableOptionsDelimiter(),
                    transform.getPostTransformConverter(),
                    supportedMetadataColumns);
        }
        postTransformFunctionBuilder
                .addTimezone(timezone)
                .addUdfFunctions(
                        udfFunctions.stream()
                                .map(this::udfDefToUDFTuple)
                                .collect(Collectors.toList()))
                .addModelClients(loadModelClients(models));

        return input.transform(
                        "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build())
                .uid(operatorUidGenerator.generateUid("post-transform"));
    }

    /**
     * Loads AI model clients for all declared models via SPI, returning a map from Janino parameter
     * name to the client instance.
     */
    private Map<String, AiModelClient> loadModelClients(List<ModelDef> models) {
        if (models.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, AiModelClientFactory> factories = new HashMap<>();
        ServiceLoader<AiModelClientFactory> loader =
                ServiceLoader.load(
                        AiModelClientFactory.class, Thread.currentThread().getContextClassLoader());
        for (AiModelClientFactory factory : loader) {
            factories.put(factory.identifier(), factory);
        }

        Map<String, AiModelClient> clients = new LinkedHashMap<>();
        for (ModelDef model : models) {
            AiModelClientFactory factory = factories.get(model.getType());
            if (factory == null) {
                throw new IllegalArgumentException(
                        "No AiModelClientFactory found for model type '"
                                + model.getType()
                                + "'. Available factories: "
                                + factories.keySet());
            }
            ModelContext ctx =
                    new DefaultModelContext(model, Thread.currentThread().getContextClassLoader());
            factory.validate(ctx);
            AiModelClient client = factory.createClient(ctx);
            clients.put(model.getName(), client);
        }
        return clients;
    }

    private Tuple3<String, String, Map<String, String>> udfDefToUDFTuple(UdfDef udf) {
        return Tuple3.of(udf.getName(), udf.getClasspath(), udf.getOptions());
    }

    // -------------------------------------------------------------------------
    // Internal ModelContext implementation
    // -------------------------------------------------------------------------

    private static final class DefaultModelContext implements ModelContext {
        private final ModelDef modelDef;
        private final ClassLoader classLoader;

        DefaultModelContext(ModelDef modelDef, ClassLoader classLoader) {
            this.modelDef = modelDef;
            this.classLoader = classLoader;
        }

        @Override
        public String getModelName() {
            return modelDef.getName();
        }

        @Override
        public Map<String, String> getOptions() {
            return modelDef.getOptions();
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
