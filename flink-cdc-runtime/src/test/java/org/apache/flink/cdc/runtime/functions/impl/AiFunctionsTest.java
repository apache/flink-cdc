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

/** Tests for {@link AiFunctions}. */
class AiFunctionsTest {

    private static class TestModelClient
            implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {

        private static final long serialVersionUID = 1L;

        private String lastPrompt;

        @Override
        public String generate(String systemPrompt, String userInput) {
            lastPrompt = systemPrompt;
            return "{\"result\":\"ABC\"}";
        }

        @Override
        public float[] embed(String text) {
            return new float[] {0.1f, 0.2f, 0.3f};
        }
    }

    private static class UnsupportedModelClient implements AiModelClient {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void testAiFunctions() {
        TestModelClient model = new TestModelClient();

        assertThat(AiFunctions.aiComplete(model, "input", "Return three letters"))
                .hasToString("{\"result\":\"ABC\"}");
        assertThat(model.lastPrompt).contains("Return three letters").contains("\"result\"");
        assertThat(AiFunctions.aiEmbed(model, "input")).containsExactly(0.1f, 0.2f, 0.3f);
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
        TestModelClient model =
                new TestModelClient() {
                    @Override
                    public String generate(String systemPrompt, String userInput) {
                        return "not-json";
                    }
                };

        assertThatThrownBy(() -> AiFunctions.aiComplete(model, "input", "prompt"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to parse AI response as JSON");
    }
}
