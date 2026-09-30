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

package org.apache.flink.cdc.runtime.functions.impl;

import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsImageEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsImageTextGeneration;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.variant.BinaryVariant;
import org.apache.flink.cdc.common.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.cdc.runtime.ai.AiTextFunctionDef;

import org.apache.flink.shaded.guava31.com.google.common.primitives.Floats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Built-in AI functions available to transform expressions. */
public class AiFunctions {

    private static final int MAX_INVALID_JSON_RESPONSE_LENGTH = 512;

    private AiFunctions() {}

    public static BinaryVariant aiComplete(
            String modelName,
            String input,
            String systemPrompt,
            Map<String, AiModelClient> modelClients) {
        return generateText(
                modelClients, modelName, AiTextFunctionDef.AI_COMPLETE, input, systemPrompt);
    }

    public static BinaryVariant aiClassify(
            String modelName,
            String input,
            String labels,
            Map<String, AiModelClient> modelClients) {
        return generateText(modelClients, modelName, AiTextFunctionDef.AI_CLASSIFY, input, labels);
    }

    public static BinaryVariant aiTranslate(
            String modelName,
            String input,
            String sourceLang,
            String targetLang,
            Map<String, AiModelClient> modelClients) {
        return generateText(
                modelClients,
                modelName,
                AiTextFunctionDef.AI_TRANSLATE,
                input,
                sourceLang,
                targetLang);
    }

    public static BinaryVariant aiSummarize(
            String modelName,
            String input,
            int maxLength,
            Map<String, AiModelClient> modelClients) {
        return generateText(
                modelClients, modelName, AiTextFunctionDef.AI_SUMMARIZE, input, maxLength);
    }

    public static BinaryVariant aiSentiment(
            String modelName, String input, Map<String, AiModelClient> modelClients) {
        return generateText(modelClients, modelName, AiTextFunctionDef.AI_SENTIMENT, input);
    }

    public static BinaryVariant aiExtract(
            String modelName,
            String input,
            String schema,
            Map<String, AiModelClient> modelClients) {
        return generateText(modelClients, modelName, AiTextFunctionDef.AI_EXTRACT, input, schema);
    }

    public static BinaryVariant aiMask(
            String modelName,
            String input,
            String entities,
            Map<String, AiModelClient> modelClients) {
        return generateText(modelClients, modelName, AiTextFunctionDef.AI_MASK, input, entities);
    }

    private static BinaryVariant generateText(
            Map<String, AiModelClient> modelClients,
            String modelName,
            AiTextFunctionDef function,
            String input,
            Object... promptArguments) {
        if (input == null) {
            return null;
        }
        SupportsTextGeneration model =
                resolveModel(
                        modelClients,
                        modelName,
                        function.getFunctionName(),
                        SupportsTextGeneration.class);

        String prompt =
                function.buildPrompt(promptArguments)
                        + "\n"
                        + buildOutputSchemaHint(function.getOutputType());
        String json = model.generate(prompt, input);
        if (json == null) {
            return null;
        }
        try {
            return BinaryVariantInternalBuilder.parseJson(json, false);
        } catch (IOException e) {
            throw new RuntimeException(
                    "AI function "
                            + function.getFunctionName()
                            + " returned invalid JSON: "
                            + truncateInvalidJsonResponse(json),
                    e);
        }
    }

    public static List<Float> aiEmbed(
            String modelName, String input, Map<String, AiModelClient> modelClients) {
        if (input == null) {
            return null;
        }
        SupportsEmbedding model =
                resolveModel(modelClients, modelName, "AI_EMBED", SupportsEmbedding.class);
        float[] embedding = model.embed(input);
        return embedding == null ? null : Floats.asList(embedding);
    }

    /** Dispatches image-to-text AI functions. */
    public static String aiImageComplete(
            String modelName,
            byte[] image,
            String prompt,
            Map<String, AiModelClient> modelClients) {
        if (image == null) {
            return null;
        }
        SupportsImageTextGeneration model =
                resolveModel(
                        modelClients,
                        modelName,
                        "AI_IMAGE_COMPLETE",
                        SupportsImageTextGeneration.class);
        return model.generateTextFromImage(image, prompt);
    }

    /** Dispatches image embedding AI functions. */
    public static List<Float> aiImageEmbed(
            String modelName, byte[] image, Map<String, AiModelClient> modelClients) {
        if (image == null) {
            return null;
        }
        SupportsImageEmbedding model =
                resolveModel(
                        modelClients, modelName, "AI_IMAGE_EMBED", SupportsImageEmbedding.class);
        float[] embedding = model.embedImage(image);
        return embedding == null ? null : Floats.asList(embedding);
    }

    private static <T> T resolveModel(
            Map<String, AiModelClient> modelClients,
            String modelName,
            String functionName,
            Class<T> requiredCapability) {
        if (modelName == null) {
            throw new IllegalArgumentException(
                    "Model name referenced by " + functionName + " must not be null.");
        }
        AiModelClient model = modelClients.get(modelName);
        if (model == null) {
            throw new IllegalArgumentException(
                    "Model '"
                            + modelName
                            + "' referenced by "
                            + functionName
                            + " has not been declared.");
        }
        if (!requiredCapability.isInstance(model)) {
            throw new UnsupportedOperationException(
                    "Model '"
                            + modelName
                            + "' could not be used in "
                            + functionName
                            + " because it does not implement "
                            + requiredCapability.getSimpleName()
                            + " interface.");
        }
        return requiredCapability.cast(model);
    }

    private static String truncateInvalidJsonResponse(String response) {
        if (response.length() <= MAX_INVALID_JSON_RESPONSE_LENGTH) {
            return response;
        }
        return response.substring(0, MAX_INVALID_JSON_RESPONSE_LENGTH) + "... (truncated)";
    }

    private static String buildOutputSchemaHint(RowType outputType) {
        StringBuilder builder =
                new StringBuilder(
                        "Return only valid JSON without Markdown fences or additional text, using this shape:\n{\n");
        List<String> fieldNames = outputType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            builder.append("  \"")
                    .append(fieldNames.get(i))
                    .append("\": <")
                    .append(fieldNames.get(i))
                    .append(">");
            if (i < fieldNames.size() - 1) {
                builder.append(',');
            }
            builder.append('\n');
        }
        return builder.append('}').toString();
    }
}
