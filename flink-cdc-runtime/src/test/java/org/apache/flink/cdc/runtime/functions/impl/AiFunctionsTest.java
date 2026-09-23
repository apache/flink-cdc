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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AiFunctions}. */
class AiFunctionsTest {

    private static class TestModelClient
            implements AiModelClient,
                    SupportsTextGeneration,
                    SupportsEmbedding,
                    SupportsImageTextGeneration,
                    SupportsImageEmbedding {

        private static final long serialVersionUID = 1L;

        private final String response;
        private final List<String> prompts = new ArrayList<>();
        private int embedCalls;
        private int imageTextCalls;
        private int imageEmbedCalls;

        private TestModelClient() {
            this("{\"result\":\"ABC\"}");
        }

        private TestModelClient(String response) {
            this.response = response;
        }

        @Override
        public String generate(String systemPrompt, String userInput) {
            prompts.add(systemPrompt);
            return response;
        }

        @Override
        public float[] embed(String text) {
            embedCalls++;
            return new float[] {0.1f, 0.2f, 0.3f};
        }

        @Override
        public String generateTextFromImage(byte[] image, String prompt) {
            imageTextCalls++;
            return "image has " + image.length + " bytes, prompt: " + prompt;
        }

        @Override
        public float[] embedImage(byte[] image) {
            imageEmbedCalls++;
            return new float[] {0.9f, 0.8f, 0.7f};
        }
    }

    private static class UnsupportedModelClient implements AiModelClient {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void testTextAiFunctionsUseEnglishPromptsAndParseJsonResponses() {
        TestModelClient model = new TestModelClient();
        Map<String, AiModelClient> modelClients = modelClients("textModel", model);

        assertThat(
                        AiFunctions.aiComplete(
                                "textModel", "input", "Return three letters", modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiClassify("textModel", "input", "positive,negative", modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiTranslate("textModel", "input", "auto", "en", modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiSummarize("textModel", "input", 100, modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiSentiment("textModel", "input", modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiExtract("textModel", "input", "name:string", modelClients))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiMask("textModel", "input", "email,phone", modelClients))
                .hasToString("{\"result\":\"ABC\"}");

        assertThat(model.prompts).hasSize(7);
        assertThat(model.prompts.get(0)).contains("Return three letters").contains("\"result\"");
        assertThat(model.prompts.get(1))
                .contains("text classifier", "positive,negative", "\"category\"");
        assertThat(model.prompts.get(2))
                .contains("translator", "auto", "en", "\"translated_text\"");
        assertThat(model.prompts.get(3))
                .contains("text summarizer", "100 characters", "\"summary\"");
        assertThat(model.prompts.get(4))
                .contains("sentiment analyzer", "\"score\"", "\"confidence\"");
        assertThat(model.prompts.get(5))
                .contains("information extraction", "name:string", "\"extracted_json\"");
        assertThat(model.prompts.get(6)).contains("data masking", "email,phone", "\"masked_text\"");
        assertThat(model.prompts)
                .allSatisfy(
                        prompt ->
                                assertThat(prompt)
                                        .contains("Return only valid JSON")
                                        .doesNotContainPattern("\\p{IsHan}"));
    }

    @Test
    void testEmbeddingFunction() {
        TestModelClient model = new TestModelClient();
        Map<String, AiModelClient> modelClients = modelClients("embeddingModel", model);

        assertThat(AiFunctions.aiEmbed("embeddingModel", "input", modelClients))
                .containsExactly(0.1f, 0.2f, 0.3f);
        assertThat(model.embedCalls).isOne();
    }

    @Test
    void testImageAiFunctions() {
        TestModelClient model = new TestModelClient();
        Map<String, AiModelClient> modelClients = modelClients("imageModel", model);
        byte[] image = new byte[] {1, 2, 3, 4};

        assertThat(
                        AiFunctions.aiImageComplete(
                                "imageModel", image, "Describe the image", modelClients))
                .isEqualTo("image has 4 bytes, prompt: Describe the image");
        assertThat(AiFunctions.aiImageEmbed("imageModel", image, modelClients))
                .containsExactly(0.9f, 0.8f, 0.7f);
        assertThat(model.imageTextCalls).isOne();
        assertThat(model.imageEmbedCalls).isOne();
    }

    @Test
    void testUnsupportedCapabilities() {
        UnsupportedModelClient model = new UnsupportedModelClient();
        Map<String, AiModelClient> modelClients = modelClients("unsupported", model);

        assertThatThrownBy(
                        () ->
                                AiFunctions.aiComplete(
                                        "unsupported", "input", "prompt", modelClients))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Model 'unsupported'")
                .hasMessageContaining("AI_COMPLETE")
                .hasMessageContaining("does not implement SupportsTextGeneration interface");
        assertThatThrownBy(() -> AiFunctions.aiEmbed("unsupported", "input", modelClients))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Model 'unsupported'")
                .hasMessageContaining("AI_EMBED")
                .hasMessageContaining("does not implement SupportsEmbedding interface");
        assertThatThrownBy(
                        () ->
                                AiFunctions.aiImageComplete(
                                        "unsupported", new byte[] {1, 2}, "describe", modelClients))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Model 'unsupported'")
                .hasMessageContaining("AI_IMAGE_COMPLETE")
                .hasMessageContaining("does not implement SupportsImageTextGeneration interface");
        assertThatThrownBy(
                        () ->
                                AiFunctions.aiImageEmbed(
                                        "unsupported", new byte[] {1, 2}, modelClients))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Model 'unsupported'")
                .hasMessageContaining("AI_IMAGE_EMBED")
                .hasMessageContaining("does not implement SupportsImageEmbedding interface");
    }

    @Test
    void testInvalidModelName() {
        Map<String, AiModelClient> modelClients = Collections.emptyMap();

        assertThatThrownBy(
                        () ->
                                AiFunctions.aiComplete(
                                        "missingModel", "input", "prompt", modelClients))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Model 'missingModel' referenced by AI_COMPLETE has not been declared.");
        assertThatThrownBy(() -> AiFunctions.aiEmbed(null, "input", modelClients))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Model name referenced by AI_EMBED must not be null.");
    }

    @Test
    void testInvalidJsonResponse() {
        TestModelClient model = new TestModelClient("not-json");
        Map<String, AiModelClient> modelClients = modelClients("model", model);

        assertThatThrownBy(
                        () ->
                                AiFunctions.aiClassify(
                                        "model", "input", "positive,negative", modelClients))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("AI function AI_CLASSIFY returned invalid JSON: not-json");
    }

    @Test
    void testInvalidJsonResponseIsTruncated() {
        String longInvalidJson = "x".repeat(600);
        TestModelClient model = new TestModelClient(longInvalidJson);
        Map<String, AiModelClient> modelClients = modelClients("model", model);

        assertThatThrownBy(
                        () ->
                                AiFunctions.aiClassify(
                                        "model", "input", "positive,negative", modelClients))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "AI function AI_CLASSIFY returned invalid JSON: "
                                + "x".repeat(512)
                                + "... (truncated)");
    }

    @Test
    void testNullInputSkipsModelInvocation() {
        TestModelClient model = new TestModelClient();
        Map<String, AiModelClient> modelClients = modelClients("model", model);
        Map<String, AiModelClient> emptyModelClients = Collections.emptyMap();

        assertThat(AiFunctions.aiClassify("model", null, "positive,negative", modelClients))
                .isNull();
        assertThat(AiFunctions.aiEmbed("model", null, modelClients)).isNull();
        assertThat(AiFunctions.aiImageComplete("model", null, "describe", modelClients)).isNull();
        assertThat(AiFunctions.aiImageEmbed("model", null, modelClients)).isNull();
        assertThat(AiFunctions.aiComplete("missing", null, "prompt", emptyModelClients)).isNull();
        assertThat(model.prompts).isEmpty();
        assertThat(model.embedCalls).isZero();
        assertThat(model.imageTextCalls).isZero();
        assertThat(model.imageEmbedCalls).isZero();
    }

    @Test
    void testNullModelResponseReturnsNull() {
        TestModelClient model = new TestModelClient(null);
        Map<String, AiModelClient> modelClients = modelClients("model", model);

        assertThat(AiFunctions.aiSummarize("model", "input", 100, modelClients)).isNull();
        assertThat(model.prompts).hasSize(1);
    }

    private static Map<String, AiModelClient> modelClients(String modelName, AiModelClient model) {
        return Map.of(modelName, model);
    }
}
