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
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
public class TransformTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(TransformTranslator.class);

    /** Package of built-in model. */
    public static final String PREFIX_CLASSPATH_BUILT_IN_MODEL =
            "org.apache.flink.cdc.runtime.model.";

    public DataStream<Event> translatePreTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        if (transforms.isEmpty()) {
            return input;
        }
        return input.transform(
                "Transform:Schema",
                new EventTypeInfo(),
                generatePreTransform(transforms, udfFunctions, models, supportedMetadataColumns));
    }

    private PreTransformOperator generatePreTransform(
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
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

        preTransformFunctionBuilder
                .addUdfFunctions(
                        udfFunctions.stream()
                                .map(this::udfDefToUDFTuple)
                                .collect(Collectors.toList()))
                .addUdfFunctions(
                        models.stream().map(this::modelToUDFTuple).collect(Collectors.toList()));

        return preTransformFunctionBuilder.build();
    }

    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            String timezone,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns,
            SchemaColumnCaseFormat pipelineSchemaColumnCaseFormat,
            OperatorUidGenerator operatorUidGenerator) {
        final boolean needsImplicitCaseOnlyTransform =
                pipelineSchemaColumnCaseFormat != SchemaColumnCaseFormat.AS_IS;

        PostTransformOperatorBuilder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        int postTransformRulesAdded = 0;
        for (TransformDef transform : transforms) {
            if (transform.registersPostTransformRule()) {
                SchemaColumnCaseFormat caseFormat =
                        transform.getColumnCaseFormat() != null
                                ? transform.getColumnCaseFormat()
                                : pipelineSchemaColumnCaseFormat;
                postTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.getProjection(),
                        transform.getFilter(),
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions(),
                        transform.getTableOptionsDelimiter(),
                        transform.getPostTransformConverter(),
                        supportedMetadataColumns,
                        caseFormat);
                postTransformRulesAdded++;
            }
        }

        if (postTransformRulesAdded == 0 && !needsImplicitCaseOnlyTransform) {
            return input;
        }

        if (postTransformRulesAdded == 0) {
            LOG.info(
                    "Installing implicit post-transform for pipeline column-name-case={} "
                            + "(no YAML transform with projection or filter). Table inclusion pattern: {}",
                    pipelineSchemaColumnCaseFormat,
                    PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS);
            postTransformFunctionBuilder.addTransform(
                    PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS,
                    null,
                    null,
                    "",
                    "",
                    "",
                    ",",
                    "",
                    supportedMetadataColumns,
                    pipelineSchemaColumnCaseFormat);
        }

        postTransformFunctionBuilder.addTimezone(timezone);
        postTransformFunctionBuilder.addUdfFunctions(
                udfFunctions.stream().map(this::udfDefToUDFTuple).collect(Collectors.toList()));
        postTransformFunctionBuilder.addUdfFunctions(
                models.stream().map(this::modelToUDFTuple).collect(Collectors.toList()));
        return input.transform(
                        "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build())
                .uid(operatorUidGenerator.generateUid("post-transform"));
    }

    private Tuple3<String, String, java.util.Map<String, String>> modelToUDFTuple(ModelDef model) {
        return Tuple3.of(
                model.getModelName(),
                PREFIX_CLASSPATH_BUILT_IN_MODEL + model.getClassName(),
                model.getParameters());
    }

    private Tuple3<String, String, java.util.Map<String, String>> udfDefToUDFTuple(UdfDef udf) {
        return Tuple3.of(udf.getName(), udf.getClasspath(), udf.getOptions());
    }
}
