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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link AiFunctions}. */
class AiFunctionsTest {

    private static class MockModelClient
            implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {
        private static final long serialVersionUID = 1L;

        @Override
        public String generate(String systemPrompt, String userInput) {
            if (systemPrompt.contains("summarization expert")) {
                return "{\"summary\": \"This is a summary.\"}";
            }
            // Default for AI_COMPLETE
            return "{\"result\": \"hello world\"}";
        }

        @Override
        public float[] embed(String text) {
            return new float[] {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
        }
    }

    private static class UselessClient implements AiModelClient {}

    @Test
    void testAiFunctionInvocation() {
        MockModelClient model = new MockModelClient();
        assertThat(AiFunctions.aiComplete(model, "Say hello", "You are a helpful assistant."))
                .hasToString("{\"result\":\"hello world\"}");
        assertThat(AiFunctions.aiSummarize(model, "Long text here...", 100))
                .hasToString("{\"summary\":\"This is a summary.\"}");
        assertThat(AiFunctions.aiEmbed(model, "Test text"))
                .containsExactly(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
    }

    @Test
    void testUnsupportedModel() {
        UselessClient model = new UselessClient();
        assertThatThrownBy(() -> AiFunctions.aiComplete(model, "test", "prompt"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support text generation");
        assertThatThrownBy(() -> AiFunctions.aiSummarize(model, "test", 100))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support text generation");
        assertThatThrownBy(() -> AiFunctions.aiEmbed(model, "test"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support embedding");
    }

    @Test
    void testAiCompleteWithInvalidJsonResponse() {
        MockModelClient model =
                new MockModelClient() {
                    @Override
                    public String generate(String systemPrompt, String userInput) {
                        return "invalid json";
                    }
                };

        assertThatThrownBy(() -> AiFunctions.aiComplete(model, "test", "prompt"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to parse AI response as JSON");
    }

    @Test
    void testAiCompleteWithEmptyJsonResponse() {
        MockModelClient model =
                new MockModelClient() {
                    @Override
                    public String generate(String systemPrompt, String userInput) {
                        return "{}";
                    }
                };
        assertThat(AiFunctions.aiComplete(model, "test", "prompt")).hasToString("{}");
    }

    @Test
    void testAiCompleteWithNullResponse() {
        MockModelClient model =
                new MockModelClient() {
                    @Override
                    public String generate(String systemPrompt, String userInput) {
                        return null;
                    }
                };
        assertThat(AiFunctions.aiComplete(model, "test", "prompt")).isNull();
        assertThat(AiFunctions.aiSummarize(model, "test", 100)).isNull();
    }

    @Test
    void testAiFunctionCornerCase() {
        MockModelClient model = new MockModelClient();
        assertThat(AiFunctions.aiEmbed(model, "")).containsExactly(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
        assertThat(AiFunctions.aiEmbed(model, "中文嵌入测试"))
                .containsExactly(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
    }
}
