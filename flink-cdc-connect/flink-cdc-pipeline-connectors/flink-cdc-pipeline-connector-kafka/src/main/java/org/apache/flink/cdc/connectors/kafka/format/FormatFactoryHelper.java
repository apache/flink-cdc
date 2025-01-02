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

package org.apache.flink.cdc.connectors.kafka.format;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.VALUE_FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/** Helper utility for discovering formats and validating all options. */
public class FormatFactoryHelper extends FactoryHelper {

    private final Factory factory;
    private final Factory.Context context;

    protected final Set<String> consumedOptionKeys;
    protected final Configuration allOptions;

    public FormatFactoryHelper(Factory factory, Factory.Context context) {
        super(factory, context);
        this.factory = factory;
        this.context = context;
        this.allOptions = Configuration.fromMap(context.getFactoryConfiguration().toMap());

        final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
        consumedOptions.addAll(factory.requiredOptions());
        consumedOptions.addAll(factory.optionalOptions());
        this.consumedOptionKeys =
                consumedOptions.stream().map(ConfigOption::key).collect(Collectors.toSet());
    }

    public static FormatFactoryHelper createFormatFactoryHelper(
            Factory factory, Factory.Context context) {
        return new FormatFactoryHelper(factory, context);
    }

    public SerializationSchema<Event> discoverEncodingFormat(
            Class<? extends FormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
        return discoverOptionalEncodingFormat(formatFactoryClass, formatOption)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Could not find required sink format '%s'.",
                                                formatOption.key())));
    }

    public Optional<SerializationSchema<Event>> discoverOptionalEncodingFormat(
            Class<? extends FormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
        return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                .map(
                        formatFactory -> {
                            String formatPrefix = formatPrefix(formatFactory, formatOption);
                            try {
                                return formatFactory.createEncodingFormat(
                                        context,
                                        removePrefix(
                                                context.getFactoryConfiguration(), formatPrefix));
                            } catch (Throwable t) {
                                throw new ValidationException(
                                        String.format(
                                                "Error creating scan format '%s' in option space '%s'.",
                                                formatFactory.identifier(), formatPrefix),
                                        t);
                            }
                        });
    }

    public static Configuration removePrefix(Configuration configuration, String formatPrefix) {
        return Configuration.fromMap(
                configuration.toMap().entrySet().stream()
                        .map(
                                entry -> {
                                    if (entry.getKey().startsWith(formatPrefix)) {
                                        return new HashMap.SimpleEntry<>(
                                                entry.getKey().substring(formatPrefix.length()),
                                                entry.getValue());
                                    } else {
                                        return entry;
                                    }
                                })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public Optional<FormatFactory> discoverOptionalFormatFactory(
            Class<? extends FormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
        final String identifier = allOptions.get(formatOption);
        if (identifier == null) {
            return Optional.empty();
        }

        final FormatFactory factory = discoverFactory(identifier, formatFactoryClass);
        String formatPrefix = formatPrefix(factory, formatOption);
        final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
        consumedOptions.addAll(factory.requiredOptions());
        consumedOptions.addAll(factory.optionalOptions());

        consumedOptions.stream()
                .map(option -> formatPrefix + option.key())
                .forEach(consumedOptionKeys::add);

        return Optional.of(factory);
    }

    static <T extends Factory> T discoverFactory(String identifier, Class<T> factoryClass) {

        final ServiceLoader<Factory> loader = ServiceLoader.load(Factory.class);
        final List<Factory> factoryList = new ArrayList<>();

        for (Factory factory : loader) {
            if (factory != null
                    && factory.identifier().equals(identifier)
                    && factoryClass.isAssignableFrom(factory.getClass())) {
                factoryList.add(factory);
            }
        }

        if (factoryList.isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "UnSupported format \"%s\", cannot find factory with identifier \"%s\" in the classpath.\n\n"
                                    + "Available factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            identifier,
                            StreamSupport.stream(loader.spliterator(), false)
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        if (factoryList.size() > 1) {
            throw new RuntimeException(
                    String.format(
                            "Multiple factories found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryList.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) factoryList.get(0);
    }

    private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
        String formatIdentifier = formatFactory.identifier();
        final String formatOptionKey = formatOption.key();
        if (formatOptionKey.equals(VALUE_FORMAT.key())) {
            return formatIdentifier + ".";
        } else if (formatOptionKey.endsWith(FORMAT_SUFFIX)) {
            // extract the key prefix, e.g. extract 'key' from 'key.format'
            String keyPrefix =
                    formatOptionKey.substring(0, formatOptionKey.length() - FORMAT_SUFFIX.length());
            return keyPrefix + "." + formatIdentifier + ".";
        } else {
            throw new ValidationException(
                    "Format identifier key should be 'format' or suffix with '.format', "
                            + "don't support format identifier key '"
                            + formatOptionKey
                            + "'.");
        }
    }

    @Override
    public void validateExcept(String... prefixesToSkip) {
        Preconditions.checkArgument(
                prefixesToSkip.length > 0, "Prefixes to skip can not be empty.");
        final List<String> prefixesList = Arrays.asList(prefixesToSkip);
        consumedOptionKeys.addAll(
                allOptions.keySet().stream()
                        .filter(key -> prefixesList.stream().anyMatch(key::startsWith))
                        .collect(Collectors.toSet()));
        validateFactoryOptions(factory, allOptions);
        validateUnconsumedKeys(factory.identifier(), allOptions.keySet(), consumedOptionKeys);
    }
}
