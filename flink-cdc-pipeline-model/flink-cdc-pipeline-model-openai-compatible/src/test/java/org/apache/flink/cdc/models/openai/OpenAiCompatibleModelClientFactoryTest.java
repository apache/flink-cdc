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
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiCompatibleModelClientFactoryTest {

    private final OpenAiCompatibleModelClientFactory factory =
            new OpenAiCompatibleModelClientFactory();

    private Factory.Context contextWithOptions(Map<String, String> options) {
        return new FactoryHelper.DefaultContext(
                Configuration.fromMap(options),
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @Test
    void testIdentifier() {
        assertThat(factory.identifier()).isEqualTo("openai-compatible");
    }

    @Test
    void testRequiredOptions() {
        assertThat(
                        factory.requiredOptions().stream()
                                .map(ConfigOption::key)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(
                        OpenAiCompatibleModelOptions.ENDPOINT.key(),
                        OpenAiCompatibleModelOptions.API_KEY.key(),
                        OpenAiCompatibleModelOptions.MODEL_NAME.key());
    }

    @Test
    void testOptionalOptions() {
        assertThat(
                        factory.optionalOptions().stream()
                                .map(ConfigOption::key)
                                .collect(Collectors.toSet()))
                .contains(
                        OpenAiCompatibleModelOptions.SYSTEM_PROMPT.key(),
                        OpenAiCompatibleModelOptions.TEMPERATURE.key(),
                        OpenAiCompatibleModelOptions.TOP_P.key(),
                        OpenAiCompatibleModelOptions.STOP.key(),
                        OpenAiCompatibleModelOptions.MAX_TOKENS.key(),
                        OpenAiCompatibleModelOptions.PRESENCE_PENALTY.key(),
                        OpenAiCompatibleModelOptions.N.key(),
                        OpenAiCompatibleModelOptions.SEED.key(),
                        OpenAiCompatibleModelOptions.RESPONSE_FORMAT.key(),
                        OpenAiCompatibleModelOptions.EXTRA_HEADER.key(),
                        OpenAiCompatibleModelOptions.EXTRA_BODY.key(),
                        OpenAiCompatibleModelOptions.DIMENSION.key());
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
    void testCreateClientWithRequiredOptionsOnlyUsesDefaults() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");

        OpenAiCompatibleModelClient client =
                (OpenAiCompatibleModelClient) factory.createClient(contextWithOptions(options));
        assertThat(readField(client, "globalSystemPrompt")).isNull();
        assertThat(readField(client, "responseFormat")).isEqualTo("text");
    }

    @SuppressWarnings("unchecked")
    @Test
    void testCreateClientParsesExtraHeaderAndBody() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put(
                "extra-header", "{\"X-Trace-Id\":\"trace-1\",\"X-Tags\":[\"tag-a\",\"tag-b\"]}");
        options.put("extra-body", "{\"foo\":\"bar\",\"num\":1}");

        OpenAiCompatibleModelClient client =
                (OpenAiCompatibleModelClient) factory.createClient(contextWithOptions(options));
        Map<String, List<String>> extraHeaders =
                (Map<String, List<String>>) readField(client, "extraHeaders");
        Map<String, Object> extraBody = (Map<String, Object>) readField(client, "extraBody");

        assertThat(extraHeaders).containsEntry("X-Trace-Id", Arrays.asList("trace-1"));
        assertThat(extraHeaders).containsEntry("X-Tags", Arrays.asList("tag-a", "tag-b"));
        assertThat(extraBody).containsEntry("foo", "bar");
        assertThat(extraBody).containsKey("num");
    }

    @Test
    void testValidatePassesWithAllRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");

        FactoryHelper.createFactoryHelper(factory, contextWithOptions(options)).validate();
    }

    @Test
    void testValidatePassesWithOptionalSystemPrompt() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put("system-prompt", "You are a strict JSON generator.");

        FactoryHelper.createFactoryHelper(factory, contextWithOptions(options)).validate();
    }

    @Test
    void testValidateThrowsOnMissingRequiredOption() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");

        assertThatThrownBy(
                        () ->
                                FactoryHelper.createFactoryHelper(
                                                factory, contextWithOptions(options))
                                        .validate())
                .hasMessageContaining("required options are missing");
    }

    @Test
    void testValidateThrowsOnUnknownOption() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put("unknown-key", "value");

        assertThatThrownBy(
                        () ->
                                FactoryHelper.createFactoryHelper(
                                                factory, contextWithOptions(options))
                                        .validate())
                .hasMessageContaining("Unsupported options");
    }

    @Test
    void testCreateClientFailFast() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");

        AiModelClient client = factory.createClient(contextWithOptions(options));
        assertThat(client).isInstanceOf(OpenAiCompatibleModelClient.class);
        assertThat(client).isInstanceOf(SupportsTextGeneration.class);
        assertThat(client).isInstanceOf(SupportsEmbedding.class);

        assertThatThrownBy(client::open)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to perform livecheck on OpenAI model client");
    }

    @Test
    void testCreateClientThrowsOnInvalidTemperature() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put("temperature", "2.0");

        assertThatThrownBy(() -> factory.createClient(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("temperature");
    }

    @Test
    void testCreateClientThrowsOnInvalidExtraHeaderJson() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", "https://api.example.com/v1");
        options.put("api-key", "sk-test");
        options.put("model-name", "gpt-4");
        options.put("extra-header", "not-a-json");

        assertThatThrownBy(() -> factory.createClient(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extra-header");
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
