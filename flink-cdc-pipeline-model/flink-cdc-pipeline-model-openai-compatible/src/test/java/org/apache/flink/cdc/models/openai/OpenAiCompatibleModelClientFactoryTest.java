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

import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.ModelContext;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpenAiCompatibleModelClientFactory}. */
class OpenAiCompatibleModelClientFactoryTest {

    private final OpenAiCompatibleModelClientFactory factory =
            new OpenAiCompatibleModelClientFactory();

    @Test
    void testIdentifierAndOptions() {
        assertThat(factory.identifier()).isEqualTo("openai-compatible");
        assertThat(factory.requiredOptions())
                .extracting(option -> option.key())
                .containsExactlyInAnyOrder("model", "endpoint", "api-key");
        assertThat(factory.optionalOptions())
                .extracting(option -> option.key())
                .contains(
                        "system-prompt",
                        "temperature",
                        "top-p",
                        "retry-num",
                        "retry-backoff-strategy",
                        "dimension")
                .doesNotContain("model", "endpoint", "api-key");
    }

    @Test
    void testFactoryIsDiscoverable() {
        assertThat(
                        StreamSupport.stream(ServiceLoader.load(Factory.class).spliterator(), false)
                                .filter(OpenAiCompatibleModelClientFactory.class::isInstance))
                .hasSize(1);
    }

    @Test
    void testCreateClient() {
        AiModelClient client = factory.createClient(context(validOptions()));

        assertThat(client)
                .isInstanceOf(OpenAiCompatibleModelClient.class)
                .isInstanceOf(SupportsTextGeneration.class)
                .isInstanceOf(SupportsEmbedding.class);
    }

    @Test
    void testDeprecatedModelNameAlias() {
        Map<String, String> options = validOptions();
        options.put("model-name", options.remove("model"));

        assertThat(factory.createClient(context(options)))
                .isInstanceOf(OpenAiCompatibleModelClient.class);
    }

    @Test
    void testMissingRequiredOptionIsRejected() {
        Map<String, String> options = validOptions();
        options.remove("api-key");

        assertThatThrownBy(() -> factory.createClient(context(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("required options")
                .hasMessageContaining("api-key");
    }

    @Test
    void testUnknownOptionIsRejected() {
        Map<String, String> options = validOptions();
        options.put("unknown-option", "value");

        assertThatThrownBy(() -> factory.createClient(context(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options")
                .hasMessageContaining("unknown-option");
    }

    @Test
    void testBlankOptionIsRejected() {
        Map<String, String> options = validOptions();
        options.put("endpoint", "  ");

        assertThatThrownBy(() -> factory.createClient(context(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint")
                .hasMessageContaining("must not be blank");
    }

    private static Map<String, String> validOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model", "gpt-test");
        return options;
    }

    private static ModelContext context(Map<String, String> options) {
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
}
