package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.ChangeEventUtils.resolveSchemaEvolutionOptions;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

public class YamlPipelineDefinitionParser implements PipelineDefinitionParser {

    // Parent node keys
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String TRANSFORM_KEY = "transform";
    private static final String PIPELINE_KEY = "pipeline";
    private static final String MODEL_KEY = "models";

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

    // UDF related keys
    private static final String UDF_KEY = "user-defined-function";
    private static final String UDF_FUNCTION_NAME_KEY = "name";
    private static final String UDF_CLASSPATH_KEY = "classpath";

    // Model related keys
    private static final String MODEL_NAME_KEY = "name";
    private static final String MODEL_HOST_KEY = "host";
    private static final String MODEL_API_KEY = "key";

    public static final String TRANSFORM_PRIMARY_KEY_KEY = "primary-keys";
    public static final String TRANSFORM_PARTITION_KEY_KEY = "partition-keys";
    public static final String TRANSFORM_TABLE_OPTION_KEY = "table-options";

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Override
    public PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig)
            throws Exception {
        return parse(mapper.readTree(pipelineDefPath.toFile()), globalPipelineConfig);
    }

    @Override
    public PipelineDef parse(String pipelineDefText, Configuration globalPipelineConfig)
            throws Exception {
        return parse(mapper.readTree(pipelineDefText), globalPipelineConfig);
    }

    private PipelineDef parse(JsonNode pipelineDefJsonNode, Configuration globalPipelineConfig)
            throws Exception {
        SourceDef sourceDef =
                toSourceDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SOURCE_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SOURCE_KEY));

        SinkDef sinkDef =
                toSinkDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SINK_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SINK_KEY));

        List<TransformDef> transformDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(TRANSFORM_KEY))
                .ifPresent(
                        node ->
                                node.forEach(
                                        transform -> transformDefs.add(toTransformDef(transform))));

        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(ROUTE_KEY))
                .ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));

        List<UdfDef> udfDefs = new ArrayList<>();
        Optional.ofNullable(((ObjectNode) pipelineDefJsonNode.get(PIPELINE_KEY)).remove(UDF_KEY))
                .ifPresent(node -> node.forEach(udf -> udfDefs.add(toUdfDef(udf))));

        List<ModelDef> modelDefs = new ArrayList<>();
        JsonNode modelsNode = pipelineDefJsonNode.get(PIPELINE_KEY).get(MODEL_KEY);
        if (modelsNode != null) {
            modelDefs = parseModels(modelsNode);
        }

        Configuration userPipelineConfig = toPipelineConfig(pipelineDefJsonNode.get(PIPELINE_KEY));

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);

        return new PipelineDef(
                sourceDef, sinkDef, routeDefs, transformDefs, udfDefs, modelDefs, pipelineConfig);
    }

    private SourceDef toSourceDef(JsonNode sourceNode) {
        Map<String, String> sourceMap =
                mapper.convertValue(sourceNode, new TypeReference<Map<String, String>>() {});

        String type =
                checkNotNull(
                        sourceMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in source configuration",
                        TYPE_KEY);

        String name = sourceMap.remove(NAME_KEY);

        return new SourceDef(type, name, Configuration.fromMap(sourceMap));
    }

    private SinkDef toSinkDef(JsonNode sinkNode) {
        List<String> includedSETypes = new ArrayList<>();
        List<String> excludedSETypes = new ArrayList<>();

        Optional.ofNullable(sinkNode.get(INCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> includedSETypes.add(tag.asText())));

        Optional.ofNullable(sinkNode.get(EXCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> excludedSETypes.add(tag.asText())));

        Set<SchemaChangeEventType> declaredSETypes =
                resolveSchemaEvolutionOptions(includedSETypes, excludedSETypes);

        if (sinkNode instanceof ObjectNode) {
            ((ObjectNode) sinkNode).remove(INCLUDE_SCHEMA_EVOLUTION_TYPES);
            ((ObjectNode) sinkNode).remove(EXCLUDE_SCHEMA_EVOLUTION_TYPES);
        }

        Map<String, String> sinkMap =
                mapper.convertValue(sinkNode, new TypeReference<Map<String, String>>() {});

        String type =
                checkNotNull(
                        sinkMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in sink configuration",
                        TYPE_KEY);

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

    private ModelDef toModelDef(JsonNode modelNode) {
        String name =
                checkNotNull(
                                modelNode.get(MODEL_NAME_KEY),
                                "Missing required field \"%s\" in model configuration",
                                MODEL_NAME_KEY)
                        .asText();
        String host =
                checkNotNull(
                                modelNode.get(MODEL_HOST_KEY),
                                "Missing required field \"%s\" in model configuration",
                                MODEL_HOST_KEY)
                        .asText();
        String apiKey =
                checkNotNull(
                                modelNode.get(MODEL_API_KEY),
                                "Missing required field \"%s\" in model configuration",
                                MODEL_API_KEY)
                        .asText();

        return new ModelDef(name, host, apiKey);
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
        Map<String, String> pipelineConfigMap = new HashMap<>();
        pipelineConfigNode
                .fields()
                .forEachRemaining(
                        entry -> {
                            String key = entry.getKey();
                            JsonNode value = entry.getValue();
                            if (!key.equals(MODEL_KEY)) {
                                pipelineConfigMap.put(key, value.asText());
                            }
                        });
        return Configuration.fromMap(pipelineConfigMap);
    }

    private List<ModelDef> parseModels(JsonNode modelsNode) {
        List<ModelDef> modelDefs = new ArrayList<>();
        if (modelsNode != null && modelsNode.isArray()) {
            for (JsonNode modelNode : modelsNode) {
                modelDefs.add(toModelDef(modelNode));
            }
        }
        return modelDefs;
    }
}
