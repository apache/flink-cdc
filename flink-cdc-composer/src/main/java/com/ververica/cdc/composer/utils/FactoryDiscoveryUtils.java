/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.composer.utils;

import com.ververica.cdc.common.factories.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Discovery utilities for {@link Factory}. */
public class FactoryDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryDiscoveryUtils.class);

    private FactoryDiscoveryUtils() {}

    /** Returns the {@link Factory} for the given identifier. */
    @SuppressWarnings("unchecked")
    public static <T extends Factory> T getFactoryByIdentifier(
            String identifier, Class<T> factoryClass) {

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
                            "No factory found in the classpath.\n\n"
                                    + "Available factory classes are:\n\n"
                                    + "%s",
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

    /**
     * Return the path of the jar file that contains the {@link Factory} for the given identifier.
     */
    public static <T extends Factory> URL getJarPathByIdentifier(
            String identifier, Class<T> factoryClass) {
        Factory factory = getFactoryByIdentifier(identifier, factoryClass);
        return factory.getClass().getProtectionDomain().getCodeSource().getLocation();
    }
}
