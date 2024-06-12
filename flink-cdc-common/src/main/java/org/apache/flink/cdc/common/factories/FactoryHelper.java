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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.configuration.FallbackKey;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** A helper for working with {@link Factory}. */
@PublicEvolving
public class FactoryHelper<F extends Factory> {

    protected final F factory;

    protected final Configuration allOptions;

    protected final Set<String> consumedOptionKeys;

    protected final Set<String> deprecatedOptionKeys;

    public FactoryHelper(F factory, Configuration allOptions) {
        this.factory = factory;
        this.allOptions = allOptions;
        final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
        consumedOptions.addAll(factory.requiredOptions());
        consumedOptions.addAll(factory.optionalOptions());

        consumedOptionKeys =
                consumedOptions.stream().map(ConfigOption::key).collect(Collectors.toSet());
        deprecatedOptionKeys =
                consumedOptions.stream()
                        .flatMap(
                                option ->
                                        StreamSupport.stream(
                                                        option.fallbackKeys().spliterator(), false)
                                                .filter(FallbackKey::isDeprecated)
                                                .map(FallbackKey::getKey))
                        .collect(Collectors.toSet());
    }

    /** Validates the options of the factory. It checks for unconsumed option keys. */
    public void validate() {
        validateFactoryOptions(factory, allOptions);
        validateUnconsumedKeys(
                factory.identifier(),
                allOptions.keySet(),
                consumedOptionKeys,
                deprecatedOptionKeys);
    }

    /**
     * Validates the options of the factory. It checks for unconsumed option keys while ignoring the
     * options with given prefixes.
     *
     * <p>The option keys that have given prefix {@code prefixToSkip} would just be skipped for
     * validation.
     *
     * @param prefixesToSkip Set of option key prefixes to skip validation
     */
    public void validateExcept(String... prefixesToSkip) {
        Preconditions.checkArgument(
                prefixesToSkip.length > 0, "Prefixes to skip can not be empty.");
        final List<String> prefixesList = Arrays.asList(prefixesToSkip);
        consumedOptionKeys.addAll(
                allOptions.keySet().stream()
                        .filter(key -> prefixesList.stream().anyMatch(key::startsWith))
                        .collect(Collectors.toSet()));
        validate();
    }

    /** Validates unconsumed option keys. */
    public static void validateUnconsumedKeys(
            String factoryIdentifier,
            Set<String> allOptionKeys,
            Set<String> consumedOptionKeys,
            Set<String> deprecatedOptionKeys) {
        final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
        remainingOptionKeys.removeAll(consumedOptionKeys);
        if (!remainingOptionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Unsupported options found for '%s'.\n\n"
                                    + "Unsupported options:\n\n"
                                    + "%s\n\n"
                                    + "Supported options:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
                            consumedOptionKeys.stream()
                                    .map(
                                            k -> {
                                                if (deprecatedOptionKeys.contains(k)) {
                                                    return String.format("%s (deprecated)", k);
                                                }
                                                return k;
                                            })
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    /**
     * Validates the required and optional {@link ConfigOption}s of a factory.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(Factory factory, Configuration options) {
        validateFactoryOptions(factory.requiredOptions(), factory.optionalOptions(), options);
    }

    /**
     * Validates the required options and optional options.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions,
            Configuration options) {
        // currently Flink's options have no validation feature which is why we access them eagerly
        // to provoke a parsing error
        final List<String> missingRequiredOptions =
                requiredOptions.stream()
                        .filter(option -> readOption(options, option) == null)
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());

        if (!missingRequiredOptions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            String.join("\n", missingRequiredOptions)));
        }

        optionalOptions.forEach(option -> readOption(options, option));
    }

    private static <T> T readOption(Configuration options, ConfigOption<T> option) {
        try {
            return options.get(option);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Invalid value for option '%s'.", option.key()), t);
        }
    }

    /** Default implementation of {@link Factory.Context}. */
    public static class DefaultContext implements Factory.Context {

        private final Configuration factoryConfiguration;
        private final ClassLoader classLoader;
        private final Configuration pipelineConfiguration;

        public DefaultContext(
                Configuration factoryConfiguration,
                Configuration pipelineConfiguration,
                ClassLoader classLoader) {
            this.factoryConfiguration = factoryConfiguration;
            this.pipelineConfiguration = pipelineConfiguration;
            this.classLoader = classLoader;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return pipelineConfiguration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
