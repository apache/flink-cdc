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

package org.apache.flink.cdc.common.factories;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Tests for {@link FactoryHelper}. */
class FactoryHelperTests {

    private Factory getDummyFactory() {

        return new Factory() {
            @Override
            public String identifier() {
                return "dummy";
            }

            @Override
            public Set<ConfigOption<?>> requiredOptions() {
                return Sets.newHashSet(
                        ConfigOptions.key("id").intType().noDefaultValue(),
                        ConfigOptions.key("name").stringType().noDefaultValue(),
                        ConfigOptions.key("age").doubleType().noDefaultValue());
            }

            @Override
            public Set<ConfigOption<?>> optionalOptions() {
                return Sets.newHashSet(
                        ConfigOptions.key("hobby").stringType().noDefaultValue(),
                        ConfigOptions.key("location").stringType().defaultValue("Everywhere"),
                        ConfigOptions.key("misc")
                                .mapType()
                                .defaultValue(Collections.singletonMap("A", "Z")));
            }
        };
    }

    @Test
    void testCorrectConfigValidation() {
        // This is a valid configuration.
        Map<String, String> configurations = new HashMap<>();
        configurations.put("id", "1");
        configurations.put("name", "Alice");
        configurations.put("age", "17");
        configurations.put("location", "Here");

        FactoryHelper factoryHelper =
                FactoryHelper.createFactoryHelper(
                        getDummyFactory(),
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(configurations), null, null));

        factoryHelper.validate();
    }

    @Test
    void testMissingRequiredOptionConfigValidation() {
        // This configuration doesn't provide all required options.
        Map<String, String> configurations = new HashMap<>();
        configurations.put("id", "1");
        configurations.put("age", "17");
        configurations.put("location", "Here");

        FactoryHelper factoryHelper =
                FactoryHelper.createFactoryHelper(
                        getDummyFactory(),
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(configurations), null, null));

        Assertions.assertThatThrownBy(factoryHelper::validate)
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("One or more required options are missing.");
    }

    @Test
    void testIncompatibleTypeValidation() {
        // This configuration has an option with mismatched type.
        Map<String, String> configurations = new HashMap<>();
        configurations.put("id", "1");
        configurations.put("name", "Alice");
        configurations.put("age", "Not a number");
        configurations.put("location", "Here");

        FactoryHelper factoryHelper =
                FactoryHelper.createFactoryHelper(
                        getDummyFactory(),
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(configurations), null, null));

        Assertions.assertThatThrownBy(factoryHelper::validate)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not parse value 'Not a number' for key 'age'.");
    }

    @Test
    void testRedundantConfigValidation() {
        // This configuration has redundant config options.
        Map<String, String> configurations = new HashMap<>();
        configurations.put("id", "1");
        configurations.put("name", "Alice");
        configurations.put("age", "17");
        configurations.put("what", "Not a valid configOption");

        FactoryHelper factoryHelper =
                FactoryHelper.createFactoryHelper(
                        getDummyFactory(),
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(configurations), null, null));

        Assertions.assertThatThrownBy(factoryHelper::validate)
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'dummy'.");
    }

    @Test
    void testAllowedPrefixConfigValidation() {
        // This configuration has allowed prefix options.
        Map<String, String> configurations = new HashMap<>();
        configurations.put("id", "1");
        configurations.put("name", "Alice");
        configurations.put("age", "17");
        configurations.put("debezium.foo", "Some debezium options");
        configurations.put("debezium.bar", "Another debezium options");
        configurations.put("canal.baz", "Yet another debezium options");

        FactoryHelper factoryHelper =
                FactoryHelper.createFactoryHelper(
                        getDummyFactory(),
                        new FactoryHelper.DefaultContext(
                                Configuration.fromMap(configurations), null, null));

        Assertions.assertThatThrownBy(factoryHelper::validate)
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'dummy'.");

        Assertions.assertThatThrownBy(() -> factoryHelper.validateExcept("debezium."))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'dummy'.");

        Assertions.assertThatThrownBy(() -> factoryHelper.validateExcept("canal."))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'dummy'.");

        factoryHelper.validateExcept("debezium.", "canal.");
    }
}
