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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AiFunctions}. */
class AiFunctionsTest {

    private static class TestModelClient
            implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {

        private static final long serialVersionUID = 1L;

        private final String response;
        private final List<String> prompts = new ArrayList<>();
        private int embedCalls;

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
    }

    private static class UnsupportedModelClient implements AiModelClient {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void testTextAiFunctionsUseEnglishPromptsAndParseJsonResponses() {
        TestModelClient model = new TestModelClient();

        assertThat(AiFunctions.aiComplete(model, "input", "Return three letters"))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiClassify(model, "input", "positive,negative"))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiTranslate(model, "input", "auto", "en"))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiSummarize(model, "input", 100))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiSentiment(model, "input")).hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiExtract(model, "input", "name:string"))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(AiFunctions.aiMask(model, "input", "email,phone"))
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

        assertThat(AiFunctions.aiEmbed(model, "input")).containsExactly(0.1f, 0.2f, 0.3f);
        assertThat(model.embedCalls).isOne();
    }

    @Test
    void testUnsupportedCapabilities() {
        UnsupportedModelClient model = new UnsupportedModelClient();

        assertThatThrownBy(() -> AiFunctions.aiComplete(model, "input", "prompt"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support text generation");
        assertThatThrownBy(() -> AiFunctions.aiEmbed(model, "input"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support embedding");
    }

    @Test
    void testInvalidJsonResponse() {
        TestModelClient model = new TestModelClient("not-json");

        assertThatThrownBy(() -> AiFunctions.aiClassify(model, "input", "positive,negative"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("AI function AI_CLASSIFY returned invalid JSON: not-json");
    }

    @Test
    void testInvalidJsonResponseIsTruncated() {
        String longInvalidJson = "x".repeat(600);
        TestModelClient model = new TestModelClient(longInvalidJson);

        assertThatThrownBy(() -> AiFunctions.aiClassify(model, "input", "positive,negative"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "AI function AI_CLASSIFY returned invalid JSON: "
                                + "x".repeat(512)
                                + "... (truncated)");
    }

    @Test
    void testNullInputSkipsModelInvocation() {
        TestModelClient model = new TestModelClient();

        assertThat(AiFunctions.aiClassify(model, null, "positive,negative")).isNull();
        assertThat(AiFunctions.aiEmbed(model, null)).isNull();
        assertThat(model.prompts).isEmpty();
        assertThat(model.embedCalls).isZero();
    }

    @Test
    void testNullModelResponseReturnsNull() {
        TestModelClient model = new TestModelClient(null);

        assertThat(AiFunctions.aiSummarize(model, "input", 100)).isNull();
        assertThat(model.prompts).hasSize(1);
    }
}
