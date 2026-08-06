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

import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.models.ResponseFormatJsonObject;
import com.openai.models.ResponseFormatText;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.Embedding;
import com.openai.models.embeddings.EmbeddingCreateParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** AI model client that connects to any OpenAI-compatible endpoint. */
public class OpenAiCompatibleModelClient
        implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {

    private static final Logger LOG = LoggerFactory.getLogger(OpenAiCompatibleModelClient.class);

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String apiKey;
    private final String modelName;
    private final String globalSystemPrompt;
    private final Double temperature;
    private final Double topP;
    private final String stop;
    private final Integer maxTokens;
    private final Double presencePenalty;
    private final Integer n;
    private final Long seed;
    private final String responseFormat;
    private final Map<String, List<String>> extraHeaders;
    private final Map<String, Object> extraBody;
    private final Integer embeddingDimension;

    private transient OpenAIClient client;

    public OpenAiCompatibleModelClient(
            String endpoint,
            String apiKey,
            String modelName,
            String globalSystemPrompt,
            Double temperature,
            Double topP,
            String stop,
            Integer maxTokens,
            Double presencePenalty,
            Integer n,
            Long seed,
            String responseFormat,
            Map<String, List<String>> extraHeaders,
            Map<String, Object> extraBody,
            Integer embeddingDimension) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.modelName = modelName;
        this.globalSystemPrompt = globalSystemPrompt;
        this.temperature = temperature;
        this.topP = topP;
        this.stop = stop;
        this.maxTokens = maxTokens;
        this.presencePenalty = presencePenalty;
        this.n = n;
        this.seed = seed;
        this.responseFormat = responseFormat;
        this.extraHeaders = extraHeaders == null ? Collections.emptyMap() : extraHeaders;
        this.extraBody = extraBody == null ? Collections.emptyMap() : extraBody;
        this.embeddingDimension = embeddingDimension;
    }

    @Override
    public void open() {
        client = OpenAIOkHttpClient.builder().baseUrl(endpoint).apiKey(apiKey).build();
        LOG.info(
                "Successfully constructed OpenAI http client. Endpoint: {} Model: {}",
                endpoint,
                modelName);
        try {
            client.models().list();
        } catch (Exception e) {
            throw new RuntimeException("Failed to perform livecheck on OpenAI model client", e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } finally {
                client = null;
            }
        }
    }

    @Override
    public String generate(String systemPrompt, String userInput) {
        String mergedSystemPrompt = mergeSystemPrompt(globalSystemPrompt, systemPrompt);
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder()
                        .model(modelName)
                        .addSystemMessage(mergedSystemPrompt)
                        .addUserMessage(userInput);

        if (temperature != null) {
            builder.temperature(temperature);
        }
        if (topP != null) {
            builder.topP(topP);
        }
        if (stop != null && !stop.trim().isEmpty()) {
            builder.stop(stop);
        }
        if (maxTokens != null) {
            builder.maxTokens(maxTokens.longValue());
        }
        if (presencePenalty != null) {
            builder.presencePenalty(presencePenalty);
        }
        if (n != null) {
            builder.n(n.longValue());
        }
        if (seed != null) {
            builder.seed(seed);
        }
        if ("json_object".equals(responseFormat)) {
            builder.responseFormat(
                    ResponseFormatJsonObject.builder().type(JsonValue.from("json_object")).build());
        } else {
            builder.responseFormat(
                    ResponseFormatText.builder().type(JsonValue.from("text")).build());
        }
        for (Map.Entry<String, List<String>> entry : extraHeaders.entrySet()) {
            builder.putAdditionalHeaders(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : extraBody.entrySet()) {
            builder.putAdditionalBodyProperty(entry.getKey(), JsonValue.from(entry.getValue()));
        }

        ChatCompletionCreateParams params = builder.build();
        ChatCompletion completion = client.chat().completions().create(params);
        return completion.choices().get(0).message().content().orElse(null);
    }

    static String mergeSystemPrompt(String globalSystemPrompt, String runtimeSystemPrompt) {
        String normalizedGlobal = normalizePrompt(globalSystemPrompt);
        String normalizedRuntime = normalizePrompt(runtimeSystemPrompt);
        if (normalizedGlobal == null) {
            return normalizedRuntime == null ? "" : normalizedRuntime;
        }
        if (normalizedRuntime == null) {
            return normalizedGlobal;
        }
        return normalizedGlobal + "\n\n" + normalizedRuntime;
    }

    private static String normalizePrompt(String prompt) {
        if (prompt == null || prompt.trim().isEmpty()) {
            return null;
        }
        return prompt;
    }

    @Override
    public float[] embed(String text) {
        EmbeddingCreateParams.Builder builder =
                EmbeddingCreateParams.builder().model(modelName).input(text);
        if (embeddingDimension != null) {
            builder.dimensions(embeddingDimension.longValue());
        }
        for (Map.Entry<String, List<String>> entry : extraHeaders.entrySet()) {
            builder.putAdditionalHeaders(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : extraBody.entrySet()) {
            builder.putAdditionalBodyProperty(entry.getKey(), JsonValue.from(entry.getValue()));
        }
        EmbeddingCreateParams params = builder.build();
        CreateEmbeddingResponse response = client.embeddings().create(params);
        List<Embedding> data = response.data();
        if (data.isEmpty()) {
            throw new IllegalStateException(
                    "Embedding response from model '"
                            + modelName
                            + "' contained no embeddings. "
                            + "This indicates a server-side anomaly; refusing to emit an empty vector.");
        }
        List<Float> embedding = data.get(0).embedding();
        float[] result = new float[embedding.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = embedding.get(i);
        }
        return result;
    }
}
