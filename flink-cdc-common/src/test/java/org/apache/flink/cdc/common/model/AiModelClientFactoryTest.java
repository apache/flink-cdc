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

package org.apache.flink.cdc.common.model;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Tests for the default {@link AiModelClientFactory#validate} method. */
class AiModelClientFactoryTest {

    private static final String IDENTIFIER = "test-provider";
    private static final String MODEL_NAME = "my-model";

    private static final class StubFactory implements AiModelClientFactory {
        private final Set<String> required;
        private final Set<String> optional;

        StubFactory(Set<String> required, Set<String> optional) {
            this.required = required;
            this.optional = optional;
        }

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<String> requiredOptions() {
            return required;
        }

        @Override
        public Set<String> optionalOptions() {
            return optional;
        }

        @Override
        public AiModelClient createClient(ModelContext context) {
            return new AiModelClient() {};
        }
    }

    private static ModelContext contextWithOptions(Map<String, String> options) {
        return new ModelContext() {
            @Override
            public String getModelName() {
                return MODEL_NAME;
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
    void testValidatePassesWithAllRequiredOptions() {
        StubFactory factory = new StubFactory(Set.of("api-key", "endpoint"), Set.of("timeout"));

        Map<String, String> options = new HashMap<>();
        options.put("api-key", "sk-xxx");
        options.put("endpoint", "https://api.example.com");

        // Should not throw
        factory.validate(contextWithOptions(options));
    }

    @Test
    void testValidatePassesWithRequiredAndOptionalOptions() {
        StubFactory factory = new StubFactory(Set.of("api-key", "endpoint"), Set.of("timeout"));

        Map<String, String> options = new HashMap<>();
        options.put("api-key", "sk-xxx");
        options.put("endpoint", "https://api.example.com");
        options.put("timeout", "30000");

        factory.validate(contextWithOptions(options));
    }

    @Test
    void testValidateThrowsOnMissingRequiredOption() {
        StubFactory factory = new StubFactory(Set.of("api-key", "endpoint"), Set.of("timeout"));

        // Missing "endpoint"
        Map<String, String> options = new HashMap<>();
        options.put("api-key", "sk-xxx");

        Assertions.assertThatThrownBy(() -> factory.validate(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Missing required options for model 'my-model' (type='test-provider'): [endpoint]");
    }

    @Test
    void testValidateThrowsOnMultipleMissingRequiredOptions() {
        StubFactory factory = new StubFactory(Set.of("api-key", "endpoint", "model"), Set.of());

        // All required options missing
        Assertions.assertThatThrownBy(
                        () -> factory.validate(contextWithOptions(Collections.emptyMap())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Missing required options for model 'my-model' (type='test-provider'): [endpoint, api-key, model]");
    }

    @Test
    void testValidateThrowsOnUnknownOption() {
        StubFactory factory = new StubFactory(Set.of("api-key"), Set.of("timeout"));

        Map<String, String> options = new HashMap<>();
        options.put("api-key", "sk-xxx");
        options.put("bogus", "unexpected");

        Assertions.assertThatThrownBy(() -> factory.validate(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown options for model 'my-model' (type='test-provider'): [bogus]");
    }

    @Test
    void testValidateThrowsOnMultipleUnknownOptions() {
        StubFactory factory = new StubFactory(Set.of("api-key"), Set.of());

        Map<String, String> options = new HashMap<>();
        options.put("api-key", "sk-xxx");
        options.put("foo", "a");
        options.put("bar", "b");

        Assertions.assertThatThrownBy(() -> factory.validate(contextWithOptions(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unknown options for model 'my-model' (type='test-provider'): [bar, foo]");
    }

    @Test
    void testValidatePassesWithNoRequiredAndNoOptions() {
        StubFactory factory = new StubFactory(Set.of(), Set.of());
        factory.validate(contextWithOptions(Collections.emptyMap()));
    }
}
