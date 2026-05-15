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
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.Embedding;
import com.openai.models.embeddings.EmbeddingCreateParams;

import java.util.List;

/** AI model client that connects to any OpenAI-compatible endpoint. */
public class OpenAiCompatibleModelClient
        implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String apiKey;
    private final String modelName;

    private transient OpenAIClient client;

    public OpenAiCompatibleModelClient(String endpoint, String apiKey, String modelName) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.modelName = modelName;
    }

    @Override
    public void open() {
        client = OpenAIOkHttpClient.builder().baseUrl(endpoint).apiKey(apiKey).build();
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
        ChatCompletionCreateParams params =
                ChatCompletionCreateParams.builder()
                        .model(modelName)
                        .addSystemMessage(systemPrompt)
                        .addUserMessage(userInput)
                        .build();
        ChatCompletion completion = client.chat().completions().create(params);
        return completion.choices().get(0).message().content().orElse(null);
    }

    @Override
    public float[] embed(String text) {
        EmbeddingCreateParams params =
                EmbeddingCreateParams.builder().model(modelName).input(text).build();
        CreateEmbeddingResponse response = client.embeddings().create(params);
        List<Embedding> data = response.data();
        if (data.isEmpty()) {
            return new float[0];
        }
        List<Float> embedding = data.get(0).embedding();
        float[] result = new float[embedding.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = embedding.get(i);
        }
        return result;
    }
}
