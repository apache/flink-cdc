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
import org.apache.flink.cdc.common.model.ModelContext;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiCompatibleModelClientFactoryTest {

    private final OpenAiCompatibleModelClientFactory factory =
            new OpenAiCompatibleModelClientFactory();

    private ModelContext contextWithOptions(Map<String, String> options) {
        return new ModelContext() {
            @Override
            public String getModelName() {
                return "test-model";
            }

            @Override
            public Map<String, String> getOptions() {
                return options;
            }

            @Override
            public ClassLoader getClassLoader() {
                return Thread.currentThread().getContextClassLoader();
            }
        };
    }

    @Test
    void testIdentifier() {
        assertThat(factory.identifier()).isEqualTo("openai-compatible");
    }

    @Test
    void testRequiredOptions() {
        assertThat(factory.requiredOptions())
                .containsExactlyInAnyOrder("endpoint", "api-key", "model-name");
    }

    @Test
    void testOptionalOptions() {
        assertThat(factory.optionalOptions()).isEmpty();
    }

    @Test
    void testCreateClient() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");

        AiModelClient client = factory.createClient(contextWithOptions(options));
        assertThat(client).isInstanceOf(OpenAiCompatibleModelClient.class);
        assertThat(client).isInstanceOf(SupportsTextGeneration.class);
        assertThat(client).isInstanceOf(SupportsEmbedding.class);
    }

    @Test
    void testValidatePassesWithAllRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");

        factory.validate(contextWithOptions(options));
    }

    @Test
    void testValidateThrowsOnMissingRequiredOption() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");

        assertThatThrownBy(() -> factory.validate(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing required options");
    }

    @Test
    void testValidateThrowsOnUnknownOption() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put("unknown-key", "value");

        assertThatThrownBy(() -> factory.validate(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown options");
    }
}
