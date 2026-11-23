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
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.AiModelClientFactory;
import org.apache.flink.cdc.common.model.ModelContext;

import java.util.Set;
import java.util.stream.Collectors;

/** SPI factory for {@link OpenAiCompatibleModelClient}. */
public class OpenAiCompatibleModelClientFactory implements AiModelClientFactory {

    private static final Set<ConfigOption<?>> REQUIRED_OPTIONS =
            Set.of(
                    OpenAiCompatibleModelOptions.MODEL,
                    OpenAiCompatibleModelOptions.ENDPOINT,
                    OpenAiCompatibleModelOptions.API_KEY);

    private static final Set<ConfigOption<?>> OPTIONAL_OPTIONS =
            OpenAiCompatibleModelOptions.ALL_OPTIONS.stream()
                    .filter(option -> !REQUIRED_OPTIONS.contains(option))
                    .collect(Collectors.toSet());

    @Override
    public String identifier() {
        return "openai-compatible";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return REQUIRED_OPTIONS;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return OPTIONAL_OPTIONS;
    }

    @Override
    public AiModelClient createClient(ModelContext context) {
        validate(context);
        Configuration options = Configuration.fromMap(context.getOptions());
        String endpoint =
                requireNonBlank(options.get(OpenAiCompatibleModelOptions.ENDPOINT), "endpoint");
        String apiKey =
                requireNonBlank(options.get(OpenAiCompatibleModelOptions.API_KEY), "api-key");
        String model = requireNonBlank(options.get(OpenAiCompatibleModelOptions.MODEL), "model");
        String systemPrompt = options.get(OpenAiCompatibleModelOptions.SYSTEM_PROMPT);
        OpenAiRequestParams params = OpenAiRequestParams.fromOptions(options);
        return new OpenAiCompatibleModelClient(endpoint, apiKey, model, systemPrompt, params);
    }

    void validate(ModelContext context) {
        FactoryHelper.createFactoryHelper(
                        this,
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(context.getOptions()),
                                new Configuration(),
                                context.getClassLoader()))
                .validate();
    }

    private static String requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Option '%s' must not be blank.", option));
        }
        return value;
    }
}
