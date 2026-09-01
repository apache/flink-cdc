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
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.variant.BinaryVariant;
import org.apache.flink.cdc.common.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.cdc.runtime.ai.AiTextFunctionDef;

import org.apache.flink.shaded.guava31.com.google.common.primitives.Floats;

import java.io.IOException;
import java.util.List;

/** Built-in AI functions available to transform expressions. */
public class AiFunctions {

    private static final int MAX_INVALID_JSON_RESPONSE_LENGTH = 512;

    private AiFunctions() {}

    public static BinaryVariant aiComplete(AiModelClient model, String input, String systemPrompt) {
        return generateText(model, AiTextFunctionDef.AI_COMPLETE, input, systemPrompt);
    }

    public static BinaryVariant aiClassify(AiModelClient model, String input, String labels) {
        return generateText(model, AiTextFunctionDef.AI_CLASSIFY, input, labels);
    }

    public static BinaryVariant aiTranslate(
            AiModelClient model, String input, String sourceLang, String targetLang) {
        return generateText(model, AiTextFunctionDef.AI_TRANSLATE, input, sourceLang, targetLang);
    }

    public static BinaryVariant aiSummarize(AiModelClient model, String input, int maxLength) {
        return generateText(model, AiTextFunctionDef.AI_SUMMARIZE, input, maxLength);
    }

    public static BinaryVariant aiSentiment(AiModelClient model, String input) {
        return generateText(model, AiTextFunctionDef.AI_SENTIMENT, input);
    }

    public static BinaryVariant aiExtract(AiModelClient model, String input, String schema) {
        return generateText(model, AiTextFunctionDef.AI_EXTRACT, input, schema);
    }

    public static BinaryVariant aiMask(AiModelClient model, String input, String entities) {
        return generateText(model, AiTextFunctionDef.AI_MASK, input, entities);
    }

    private static BinaryVariant generateText(
            AiModelClient model,
            AiTextFunctionDef function,
            String input,
            Object... promptArguments) {
        if (input == null) {
            return null;
        }
        if (!(model instanceof SupportsTextGeneration)) {
            throw new UnsupportedOperationException(
                    "Model " + model.getClass().getName() + " does not support text generation");
        }

        String prompt =
                function.buildPrompt(promptArguments)
                        + "\n"
                        + buildOutputSchemaHint(function.getOutputType());
        String json = ((SupportsTextGeneration) model).generate(prompt, input);
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

    public static List<Float> aiEmbed(AiModelClient model, String input) {
        if (input == null) {
            return null;
        }
        if (!(model instanceof SupportsEmbedding)) {
            throw new UnsupportedOperationException(
                    "Model " + model.getClass().getName() + " does not support embedding");
        }
        float[] embedding = ((SupportsEmbedding) model).embed(input);
        return embedding == null ? null : Floats.asList(embedding);
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
