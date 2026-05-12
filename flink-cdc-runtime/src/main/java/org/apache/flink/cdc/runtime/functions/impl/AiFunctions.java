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

import java.util.List;

/** Built-in AI functions available as static imports in Janino-compiled transform expressions. */
public class AiFunctions {

    /** General-purpose text completion with a user-provided system prompt. */
    public static BinaryVariant aiComplete(AiModelClient model, String input, String systemPrompt) {
        return invokeTextGeneration(model, AiTextFunctionDef.AI_COMPLETE, input, systemPrompt);
    }

    /** Text summarization with a maximum length constraint. */
    public static BinaryVariant aiSummarize(AiModelClient model, String input, int maxLength) {
        return invokeTextGeneration(model, AiTextFunctionDef.AI_SUMMARIZE, input, maxLength);
    }

    /** Text embedding that converts input text to a vector representation. */
    public static List<Float> aiEmbed(AiModelClient model, String input) {
        if (!(model instanceof SupportsEmbedding)) {
            throw new UnsupportedOperationException(
                    "Model " + model.getClass().getName() + " does not support embedding");
        }
        return Floats.asList(((SupportsEmbedding) model).embed(input));
    }

    private static BinaryVariant invokeTextGeneration(
            AiModelClient model, AiTextFunctionDef funcDef, String input, Object... args) {
        if (!(model instanceof SupportsTextGeneration)) {
            throw new UnsupportedOperationException(
                    "Model " + model.getClass().getName() + " does not support text generation");
        }
        StringBuilder promptBuilder = new StringBuilder();
        promptBuilder.append(funcDef.buildPrompt(args));
        promptBuilder.append("\n").append(buildOutputSchemaHint(funcDef.getOutputType()));

        String systemPrompt = promptBuilder.toString();
        String json = ((SupportsTextGeneration) model).generate(systemPrompt, input);
        if (json == null) {
            return null;
        }
        try {
            return BinaryVariantInternalBuilder.parseJson(json, false);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to parse AI response as JSON: " + json, e);
        }
    }

    /** Builds the JSON output schema hint based on outputType. */
    private static String buildOutputSchemaHint(RowType outputType) {
        StringBuilder sb = new StringBuilder();
        sb.append("You must return the result strictly in the following JSON format:\n");
        sb.append("{\n");
        List<String> fieldNames = outputType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            sb.append("  \"")
                    .append(fieldNames.get(i))
                    .append("\": <")
                    .append(fieldNames.get(i))
                    .append(">");
            if (i < fieldNames.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("}\n");
        sb.append(
                "Important: Return only valid JSON with no additional text, without YAML code blocks.\n");
        return sb.toString();
    }
}
