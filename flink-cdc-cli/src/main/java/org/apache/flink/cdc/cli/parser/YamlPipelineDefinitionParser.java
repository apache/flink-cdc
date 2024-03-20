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
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** Parser for converting YAML formatted pipeline definition to {@link PipelineDef}. */
public class YamlPipelineDefinitionParser implements PipelineDefinitionParser {

    // Parent node keys
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String TRANSFORM_KEY = "transform";
    private static final String PIPELINE_KEY = "pipeline";

    // Source / sink keys
    private static final String TYPE_KEY = "type";
    private static final String NAME_KEY = "name";

    // Route keys
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_DESCRIPTION_KEY = "description";

    // Transform keys
    private static final String TRANSFORM_SOURCE_TABLE_KEY = "source-table";
    private static final String TRANSFORM_PROJECTION_KEY = "projection";
    private static final String TRANSFORM_FILTER_KEY = "filter";
    private static final String TRANSFORM_DESCRIPTION_KEY = "description";

    public static final String TRANSFORM_PRIMARY_KEY_KEY = "primary-keys";

    public static final String TRANSFORM_PARTITION_KEY_KEY = "partition-keys";

    public static final String TRANSFORM_TABLE_OPTION_KEY = "table-options";

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    /** Parse the specified pipeline definition file. */
    @Override
    public PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig)
            throws Exception {
        JsonNode root = mapper.readTree(pipelineDefPath.toFile());

        // Source is required
        SourceDef sourceDef =
                toSourceDef(
                        checkNotNull(
                                root.get(SOURCE_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SOURCE_KEY));

        // Sink is required
        SinkDef sinkDef =
                toSinkDef(
                        checkNotNull(
                                root.get(SINK_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SINK_KEY));

        // Transforms are optional
        List<TransformDef> transformDefs = new ArrayList<>();
        Optional.ofNullable(root.get(TRANSFORM_KEY))
                .ifPresent(
                        node ->
                                node.forEach(
                                        transform -> transformDefs.add(toTransformDef(transform))));

        // Routes are optional
        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(root.get(ROUTE_KEY))
                .ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));

        // Pipeline configs are optional
        Configuration userPipelineConfig = toPipelineConfig(root.get(PIPELINE_KEY));

        // Merge user config into global config
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);

        return new PipelineDef(sourceDef, sinkDef, routeDefs, transformDefs, pipelineConfig);
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

    private SinkDef toSinkDef(JsonNode sinkNode) {
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

        return new SinkDef(type, name, Configuration.fromMap(sinkMap));
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
        String description =
                Optional.ofNullable(routeNode.get(ROUTE_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        return new RouteDef(sourceTable, sinkTable, description);
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

        return new TransformDef(
                sourceTable,
                projection,
                filter,
                primaryKeys,
                partitionKeys,
                tableOptions,
                description);
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
}
