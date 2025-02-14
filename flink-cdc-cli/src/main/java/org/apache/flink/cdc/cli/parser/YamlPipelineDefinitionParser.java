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

package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR;
import static org.apache.flink.cdc.common.utils.ChangeEventUtils.resolveSchemaEvolutionOptions;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** Parser for converting YAML formatted pipeline definition to {@link PipelineDef}. */
public class YamlPipelineDefinitionParser implements PipelineDefinitionParser {

    // Parent node keys
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String TRANSFORM_KEY = "transform";
    private static final String PIPELINE_KEY = "pipeline";
    private static final String MODEL_KEY = "model";

    // Source / sink keys
    private static final String TYPE_KEY = "type";
    private static final String NAME_KEY = "name";
    private static final String INCLUDE_SCHEMA_EVOLUTION_TYPES = "include.schema.changes";
    private static final String EXCLUDE_SCHEMA_EVOLUTION_TYPES = "exclude.schema.changes";

    // Route keys
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_REPLACE_SYMBOL = "replace-symbol";
    private static final String ROUTE_DESCRIPTION_KEY = "description";

    // Transform keys
    private static final String TRANSFORM_SOURCE_TABLE_KEY = "source-table";
    private static final String TRANSFORM_PROJECTION_KEY = "projection";
    private static final String TRANSFORM_FILTER_KEY = "filter";
    private static final String TRANSFORM_DESCRIPTION_KEY = "description";
    private static final String TRANSFORM_CONVERTER_AFTER_TRANSFORM_KEY =
            "converter-after-transform";

    // UDF related keys
    private static final String UDF_KEY = "user-defined-function";
    private static final String UDF_FUNCTION_NAME_KEY = "name";
    private static final String UDF_CLASSPATH_KEY = "classpath";

    // Model related keys
    private static final String MODEL_NAME_KEY = "model-name";

    private static final String MODEL_CLASS_NAME_KEY = "class-name";

    public static final String TRANSFORM_PRIMARY_KEY_KEY = "primary-keys";

    public static final String TRANSFORM_PARTITION_KEY_KEY = "partition-keys";

    public static final String TRANSFORM_TABLE_OPTION_KEY = "table-options";

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    /** Parse the specified pipeline definition file. */
    @Override
    public PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig)
            throws Exception {
        FileSystem fileSystem = FileSystem.get(pipelineDefPath.toUri());
        FSDataInputStream pipelineInStream = fileSystem.open(pipelineDefPath);
        return parse(mapper.readTree(pipelineInStream), globalPipelineConfig);
    }

    @Override
    public PipelineDef parse(String pipelineDefText, Configuration globalPipelineConfig)
            throws Exception {
        return parse(mapper.readTree(pipelineDefText), globalPipelineConfig);
    }

    private PipelineDef parse(JsonNode pipelineDefJsonNode, Configuration globalPipelineConfig)
            throws Exception {

        // UDFs are optional. We parse UDF first and remove it from the pipelineDefJsonNode since
        // it's not of plain data types and must be removed before calling toPipelineConfig.
        List<UdfDef> udfDefs = new ArrayList<>();
        final List<ModelDef> modelDefs = new ArrayList<>();
        if (pipelineDefJsonNode.get(PIPELINE_KEY) != null) {
            Optional.ofNullable(
                            ((ObjectNode) pipelineDefJsonNode.get(PIPELINE_KEY)).remove(UDF_KEY))
                    .ifPresent(node -> node.forEach(udf -> udfDefs.add(toUdfDef(udf))));

            Optional.ofNullable(
                            ((ObjectNode) pipelineDefJsonNode.get(PIPELINE_KEY)).remove(MODEL_KEY))
                    .ifPresent(node -> modelDefs.addAll(parseModels(node)));
        }

        // Pipeline configs are optional
        Configuration userPipelineConfig = toPipelineConfig(pipelineDefJsonNode.get(PIPELINE_KEY));

        SchemaChangeBehavior schemaChangeBehavior =
                userPipelineConfig.get(PIPELINE_SCHEMA_CHANGE_BEHAVIOR);

        // Source is required
        SourceDef sourceDef =
                toSourceDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SOURCE_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SOURCE_KEY));

        // Sink is required
        SinkDef sinkDef =
                toSinkDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SINK_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SINK_KEY),
                        schemaChangeBehavior);

        // Transforms are optional
        List<TransformDef> transformDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(TRANSFORM_KEY))
                .ifPresent(
                        node ->
                                node.forEach(
                                        transform -> transformDefs.add(toTransformDef(transform))));

        // Routes are optional
        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(ROUTE_KEY))
                .ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));

        // Merge user config into global config
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);

        return new PipelineDef(
                sourceDef, sinkDef, routeDefs, transformDefs, udfDefs, modelDefs, pipelineConfig);
    }

    private SourceDef toSourceDef(JsonNode sourceNode) {
        Map<String, String> sourceMap =
                mapper.convertValue(sourceNode, new TypeReference<Map<String, String>>() {});

        // "type" field is required
        String type =
                checkNotNull(
                        sourceMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in source configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sourceMap.remove(NAME_KEY);

        return new SourceDef(type, name, Configuration.fromMap(sourceMap));
    }

    private SinkDef toSinkDef(JsonNode sinkNode, SchemaChangeBehavior schemaChangeBehavior) {
        List<String> includedSETypes = new ArrayList<>();
        List<String> excludedSETypes = new ArrayList<>();
        boolean excludedFieldNotPresent = sinkNode.get(EXCLUDE_SCHEMA_EVOLUTION_TYPES) == null;

        Optional.ofNullable(sinkNode.get(INCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> includedSETypes.add(tag.asText())));

        Optional.ofNullable(sinkNode.get(EXCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> excludedSETypes.add(tag.asText())));

        if (includedSETypes.isEmpty()) {
            // If no schema evolution types are specified, include all schema evolution types by
            // default.
            Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                    .map(SchemaChangeEventType::getTag)
                    .forEach(includedSETypes::add);
        }

        if (excludedFieldNotPresent && SchemaChangeBehavior.LENIENT.equals(schemaChangeBehavior)) {
            // In lenient mode, we exclude DROP_TABLE and TRUNCATE_TABLE by default. This could be
            // overridden by manually specifying excluded types.
            Stream.of(SchemaChangeEventType.DROP_TABLE, SchemaChangeEventType.TRUNCATE_TABLE)
                    .map(SchemaChangeEventType::getTag)
                    .forEach(excludedSETypes::add);
        }

        Set<SchemaChangeEventType> declaredSETypes =
                resolveSchemaEvolutionOptions(includedSETypes, excludedSETypes);

        if (sinkNode instanceof ObjectNode) {
            ((ObjectNode) sinkNode).remove(INCLUDE_SCHEMA_EVOLUTION_TYPES);
            ((ObjectNode) sinkNode).remove(EXCLUDE_SCHEMA_EVOLUTION_TYPES);
        }

        Map<String, String> sinkMap =
                mapper.convertValue(sinkNode, new TypeReference<Map<String, String>>() {});

        // "type" field is required
        String type =
                checkNotNull(
                        sinkMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in sink configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sinkMap.remove(NAME_KEY);

        return new SinkDef(type, name, Configuration.fromMap(sinkMap), declaredSETypes);
    }

    private RouteDef toRouteDef(JsonNode routeNode) {
        String sourceTable =
                checkNotNull(
                                routeNode.get(ROUTE_SOURCE_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SOURCE_TABLE_KEY)
                        .asText();
        String sinkTable =
                checkNotNull(
                                routeNode.get(ROUTE_SINK_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SINK_TABLE_KEY)
                        .asText();
        String replaceSymbol =
                Optional.ofNullable(routeNode.get(ROUTE_REPLACE_SYMBOL))
                        .map(JsonNode::asText)
                        .orElse(null);
        String description =
                Optional.ofNullable(routeNode.get(ROUTE_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        return new RouteDef(sourceTable, sinkTable, replaceSymbol, description);
    }

    private UdfDef toUdfDef(JsonNode udfNode) {
        String functionName =
                checkNotNull(
                                udfNode.get(UDF_FUNCTION_NAME_KEY),
                                "Missing required field \"%s\" in UDF configuration",
                                UDF_FUNCTION_NAME_KEY)
                        .asText();
        String classpath =
                checkNotNull(
                                udfNode.get(UDF_CLASSPATH_KEY),
                                "Missing required field \"%s\" in UDF configuration",
                                UDF_CLASSPATH_KEY)
                        .asText();

        return new UdfDef(functionName, classpath);
    }

    private TransformDef toTransformDef(JsonNode transformNode) {
        String sourceTable =
                checkNotNull(
                                transformNode.get(TRANSFORM_SOURCE_TABLE_KEY),
                                "Missing required field \"%s\" in transform configuration",
                                TRANSFORM_SOURCE_TABLE_KEY)
                        .asText();
        String projection =
                Optional.ofNullable(transformNode.get(TRANSFORM_PROJECTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        // When the star is in the first place, a backslash needs to be added for escape.
        if (!StringUtils.isNullOrWhitespaceOnly(projection) && projection.contains("\\*")) {
            projection = projection.replace("\\*", "*");
        }
        String filter =
                Optional.ofNullable(transformNode.get(TRANSFORM_FILTER_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String primaryKeys =
                Optional.ofNullable(transformNode.get(TRANSFORM_PRIMARY_KEY_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String partitionKeys =
                Optional.ofNullable(transformNode.get(TRANSFORM_PARTITION_KEY_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String tableOptions =
                Optional.ofNullable(transformNode.get(TRANSFORM_TABLE_OPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String description =
                Optional.ofNullable(transformNode.get(TRANSFORM_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String postTransformConverter =
                Optional.ofNullable(transformNode.get(TRANSFORM_CONVERTER_AFTER_TRANSFORM_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);

        return new TransformDef(
                sourceTable,
                projection,
                filter,
                primaryKeys,
                partitionKeys,
                tableOptions,
                description,
                postTransformConverter);
    }

    private Configuration toPipelineConfig(JsonNode pipelineConfigNode) {
        if (pipelineConfigNode == null || pipelineConfigNode.isNull()) {
            return new Configuration();
        }
        Map<String, String> pipelineConfigMap =
                mapper.convertValue(
                        pipelineConfigNode, new TypeReference<Map<String, String>>() {});
        return Configuration.fromMap(pipelineConfigMap);
    }

    private List<ModelDef> parseModels(JsonNode modelsNode) {
        List<ModelDef> modelDefs = new ArrayList<>();
        Preconditions.checkNotNull(modelsNode, "`model` in `pipeline` should not be empty.");
        if (modelsNode.isArray()) {
            for (JsonNode modelNode : modelsNode) {
                modelDefs.add(convertJsonNodeToModelDef(modelNode));
            }
        } else {
            modelDefs.add(convertJsonNodeToModelDef(modelsNode));
        }
        return modelDefs;
    }

    private ModelDef convertJsonNodeToModelDef(JsonNode modelNode) {
        String name =
                checkNotNull(
                                modelNode.get(MODEL_NAME_KEY),
                                "Missing required field \"%s\" in `model`",
                                MODEL_NAME_KEY)
                        .asText();
        String model =
                checkNotNull(
                                modelNode.get(MODEL_CLASS_NAME_KEY),
                                "Missing required field \"%s\" in `model`",
                                MODEL_CLASS_NAME_KEY)
                        .asText();
        Map<String, String> properties = mapper.convertValue(modelNode, Map.class);
        return new ModelDef(name, model, properties);
    }
}
