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
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A helper for working with {@link Factory}. */
@PublicEvolving
public class FactoryHelper {

    private final Factory factory;
    private final Factory.Context context;

    private FactoryHelper(Factory factory, Factory.Context context) {
        this.factory = factory;
        this.context = context;
    }

    public static FactoryHelper createFactoryHelper(Factory factory, Factory.Context context) {
        return new FactoryHelper(factory, context);
    }

    /**
     * Validates the required and optional {@link ConfigOption}s of a factory.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(Factory factory, Configuration configuration) {
        validateFactoryOptions(factory.requiredOptions(), factory.optionalOptions(), configuration);
    }

    /**
     * Validates the required options and optional options.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions,
            Configuration configuration) {
        final List<String> missingRequiredOptions =
                requiredOptions.stream()
                        .filter(option -> configuration.get(option) == null)
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

        optionalOptions.forEach(configuration::getOptional);
    }

    /** Validates unconsumed option keys. */
    public static void validateUnconsumedKeys(
            String factoryIdentifier, Set<String> allOptionKeys, Set<String> consumedOptionKeys) {
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
                            String.join("\n", consumedOptionKeys)));
        }
    }

    /** Validates the options of the factory. It checks for unconsumed option keys. */
    public void validate() {
        Set<String> allOptionKeys =
                Stream.concat(
                                factory.requiredOptions().stream().map(ConfigOption::key),
                                factory.optionalOptions().stream().map(ConfigOption::key))
                        .collect(Collectors.toSet());

        validateFactoryOptions(factory, context.getFactoryConfiguration());
        validateUnconsumedKeys(
                factory.identifier(), context.getFactoryConfiguration().getKeys(), allOptionKeys);
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

        Set<String> allOptionKeys =
                Stream.concat(
                                factory.requiredOptions().stream().map(ConfigOption::key),
                                factory.optionalOptions().stream().map(ConfigOption::key))
                        .collect(Collectors.toSet());

        Set<String> filteredOptionKeys =
                context.getFactoryConfiguration().getKeys().stream()
                        .filter(key -> prefixesList.stream().noneMatch(key::startsWith))
                        .collect(Collectors.toSet());

        validateFactoryOptions(factory, context.getFactoryConfiguration());
        validateUnconsumedKeys(factory.identifier(), filteredOptionKeys, allOptionKeys);
    }

    public ReadableConfig getFormatConfig(String formatPrefix) {
        final String prefix = formatPrefix + ".";
        Map<String, String> formatConfigMap = new HashMap<>();
        context.getFactoryConfiguration()
                .toMap()
                .forEach(
                        (k, v) -> {
                            if (k.startsWith(prefix)) {
                                formatConfigMap.put(k.substring(prefix.length()), v);
                            }
                        });
        return org.apache.flink.configuration.Configuration.fromMap(formatConfigMap);
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
