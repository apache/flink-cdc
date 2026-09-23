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

package org.apache.flink.cdc.models.dummy;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.ModelContext;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DummyModelClientFactory}. */
class DummyModelClientFactoryTest {

    @Test
    void testFactoryHelperValidationAndClientCreation() {
        DummyModelClientFactory factory = new DummyModelClientFactory();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Configuration options = Configuration.fromMap(Collections.singletonMap("debug", "true"));

        FactoryHelper.createFactoryHelper(
                        factory,
                        new FactoryHelper.DefaultContext(options, new Configuration(), classLoader))
                .validate();
        AiModelClient client =
                factory.createClient(new TestingModelContext(options.toMap(), classLoader));

        assertThat(client).isInstanceOf(DummyModelClient.class);
    }

    @Test
    void testUnknownOptionIsRejected() {
        DummyModelClientFactory factory = new DummyModelClientFactory();
        Configuration options =
                Configuration.fromMap(Collections.singletonMap("unknown-option", "value"));

        assertThatThrownBy(
                        () ->
                                FactoryHelper.createFactoryHelper(
                                                factory,
                                                new FactoryHelper.DefaultContext(
                                                        options,
                                                        new Configuration(),
                                                        Thread.currentThread()
                                                                .getContextClassLoader()))
                                        .validate())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options");
    }

    private static class TestingModelContext implements ModelContext {
        private final Map<String, String> options;
        private final ClassLoader classLoader;

        private TestingModelContext(Map<String, String> options, ClassLoader classLoader) {
            this.options = options;
            this.classLoader = classLoader;
        }

        @Override
        public String getModelName() {
            return "dummy-model";
        }

        @Override
        public Map<String, String> getOptions() {
            return options;
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
