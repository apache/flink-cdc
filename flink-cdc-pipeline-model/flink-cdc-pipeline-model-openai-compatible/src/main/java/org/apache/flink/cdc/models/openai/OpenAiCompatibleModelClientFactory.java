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

package org.apache.flink.cdc.models.openai;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.AiModelClientFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.model.AiModelClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** SPI factory for {@link OpenAiCompatibleModelClient}. */
public class OpenAiCompatibleModelClientFactory implements AiModelClientFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public String identifier() {
        return "openai-compatible";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return OpenAiCompatibleModelOptions.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return OpenAiCompatibleModelOptions.optionalOptions();
    }

    @Override
    public AiModelClient createClient(Factory.Context context) {
        Configuration configuration = context.getFactoryConfiguration();
        String endpoint = configuration.get(OpenAiCompatibleModelOptions.ENDPOINT);
        String apiKey = configuration.get(OpenAiCompatibleModelOptions.API_KEY);
        String modelName = configuration.get(OpenAiCompatibleModelOptions.MODEL_NAME);
        String systemPrompt =
                configuration.getOptional(OpenAiCompatibleModelOptions.SYSTEM_PROMPT).orElse(null);
        Double temperature =
                configuration.getOptional(OpenAiCompatibleModelOptions.TEMPERATURE).orElse(null);
        Double topP = configuration.getOptional(OpenAiCompatibleModelOptions.TOP_P).orElse(null);
        String stop = configuration.getOptional(OpenAiCompatibleModelOptions.STOP).orElse(null);
        Integer maxTokens =
                configuration.getOptional(OpenAiCompatibleModelOptions.MAX_TOKENS).orElse(null);
        Double presencePenalty =
                configuration
                        .getOptional(OpenAiCompatibleModelOptions.PRESENCE_PENALTY)
                        .orElse(null);
        Integer n = configuration.getOptional(OpenAiCompatibleModelOptions.N).orElse(null);
        Long seed = configuration.getOptional(OpenAiCompatibleModelOptions.SEED).orElse(null);
        String responseFormat = configuration.get(OpenAiCompatibleModelOptions.RESPONSE_FORMAT);
        String extraHeader =
                configuration.getOptional(OpenAiCompatibleModelOptions.EXTRA_HEADER).orElse(null);
        String extraBody =
                configuration.getOptional(OpenAiCompatibleModelOptions.EXTRA_BODY).orElse(null);
        Integer dimension =
                configuration.getOptional(OpenAiCompatibleModelOptions.DIMENSION).orElse(null);

        validateValueRanges(temperature, presencePenalty, n, maxTokens, dimension);
        validateEnumValue("response-format", responseFormat, "text", "json_object");

        return new OpenAiCompatibleModelClient(
                endpoint,
                apiKey,
                modelName,
                systemPrompt,
                temperature,
                topP,
                stop,
                maxTokens,
                presencePenalty,
                n,
                seed,
                responseFormat,
                parseExtraHeaders(extraHeader),
                parseExtraBody(extraBody),
                dimension);
    }

    private static void validateValueRanges(
            Double temperature,
            Double presencePenalty,
            Integer n,
            Integer maxTokens,
            Integer dimension) {
        if (temperature != null && (temperature < 0 || temperature >= 2)) {
            throw new IllegalArgumentException("Option 'temperature' must be in range [0, 2).");
        }
        if (presencePenalty != null && (presencePenalty < -2 || presencePenalty > 2)) {
            throw new IllegalArgumentException(
                    "Option 'presence-penalty' must be in range [-2.0, 2.0].");
        }
        if (n != null && n <= 0) {
            throw new IllegalArgumentException("Option 'n' must be greater than 0.");
        }
        if (maxTokens != null && maxTokens <= 0) {
            throw new IllegalArgumentException("Option 'max-tokens' must be greater than 0.");
        }
        if (dimension != null && dimension <= 0) {
            throw new IllegalArgumentException("Option 'dimension' must be greater than 0.");
        }
    }

    private static void validateEnumValue(
            String optionKey, String value, String first, String second) {
        if (!first.equals(value) && !second.equals(value)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' must be '%s' or '%s', but got '%s'.",
                            optionKey, first, second, value));
        }
    }

    private static Map<String, List<String>> parseExtraHeaders(String rawExtraHeader) {
        if (rawExtraHeader == null || rawExtraHeader.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> rawMap = parseJsonObject(rawExtraHeader, "extra-header");
        Map<String, List<String>> parsed = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                parsed.put(entry.getKey(), Collections.singletonList((String) value));
            } else if (value instanceof List<?>) {
                List<String> values = new ArrayList<>();
                for (Object element : (List<?>) value) {
                    if (!(element instanceof String)) {
                        throw new IllegalArgumentException(
                                "Option 'extra-header' only supports string values or string arrays.");
                    }
                    values.add((String) element);
                }
                parsed.put(entry.getKey(), values);
            } else {
                throw new IllegalArgumentException(
                        "Option 'extra-header' only supports string values or string arrays.");
            }
        }
        return parsed;
    }

    private static Map<String, Object> parseExtraBody(String rawExtraBody) {
        if (rawExtraBody == null || rawExtraBody.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        return parseJsonObject(rawExtraBody, "extra-body");
    }

    private static Map<String, Object> parseJsonObject(String rawJson, String optionName) {
        try {
            return OBJECT_MAPPER.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Option '%s' must be a valid JSON object string.", optionName),
                    e);
        }
    }
}
