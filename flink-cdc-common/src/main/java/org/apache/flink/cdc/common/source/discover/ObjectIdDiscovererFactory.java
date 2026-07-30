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

package org.apache.flink.cdc.common.source.discover;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** SPI factory that creates an {@link ObjectIdDiscoverer}. */
@PublicEvolving
public interface ObjectIdDiscovererFactory<ObjectIdentifier> {

    /** Storage-side type, such as jdbc, local-file, or remote-store. */
    String type();

    /** Discovered object id type, used to distinguish TableId/ObjectId/etc. */
    Class<ObjectIdentifier> objectIdClass();

    /** Creates a new uninitialized {@link ObjectIdDiscoverer}. */
    ObjectIdDiscoverer<ObjectIdentifier> createDiscoverer();

    /** Creates a discoverer context with the given configuration and class loader. */
    static ObjectIdDiscoverer.Context createContext(
            Configuration configuration, ClassLoader classLoader) {
        return new DefaultObjectIdDiscovererContext(configuration, classLoader);
    }

    /** Discovers a factory via SPI and delegates discoverer creation to it. */
    static <T> ObjectIdDiscoverer<T> createDiscoverer(
            String type, Class<T> objectIdClass, ClassLoader classLoader) {
        return discoverFactory(type, objectIdClass, classLoader).createDiscoverer();
    }

    /**
     * Discovers an {@link ObjectIdDiscovererFactory} via SPI whose {@link #type()} and {@link
     * #objectIdClass()} match the given arguments.
     *
     * @throws IllegalArgumentException if no factory matches the given combination.
     * @throws IllegalStateException if multiple factories match the given combination.
     */
    static <T> ObjectIdDiscovererFactory<T> discoverFactory(
            String type, Class<T> objectIdClass, ClassLoader classLoader) {
        ClassLoader loader =
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        return discoverFactory(type, objectIdClass, loadFactories(loader));
    }

    /**
     * Discovers an {@link ObjectIdDiscovererFactory} from the given candidates whose {@link
     * #type()} and {@link #objectIdClass()} match the given arguments.
     *
     * @throws IllegalArgumentException if no factory matches the given combination.
     * @throws IllegalStateException if multiple factories match the given combination.
     */
    static <T> ObjectIdDiscovererFactory<T> discoverFactory(
            String type,
            Class<T> objectIdClass,
            Iterable<? extends ObjectIdDiscovererFactory<?>> factories) {
        String normalizedType = normalizeType(type);
        List<ObjectIdDiscovererFactory<?>> matched = new ArrayList<>();
        List<String> available = new ArrayList<>();

        for (ObjectIdDiscovererFactory<?> factory : factories) {
            String factoryType = normalizeType(factory.type());
            Class<?> factoryObjectIdClass = factory.objectIdClass();
            available.add(formatFactory(factoryType, factoryObjectIdClass, factory));
            if (factoryType.equals(normalizedType) && factoryObjectIdClass.equals(objectIdClass)) {
                matched.add(factory);
            }
        }

        if (matched.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unsupported object id discoverer factory for type '"
                            + type
                            + "' and object id class '"
                            + objectIdClass.getName()
                            + "'. Available discoverer factories: "
                            + available
                            + ".");
        }

        if (matched.size() > 1) {
            throw new IllegalStateException(
                    "Multiple ObjectIdDiscovererFactory implementations found for type '"
                            + type
                            + "' and object id class '"
                            + objectIdClass.getName()
                            + "': "
                            + matched.stream()
                                    .map(factory -> factory.getClass().getName())
                                    .collect(Collectors.joining(", ")));
        }

        @SuppressWarnings("unchecked")
        ObjectIdDiscovererFactory<T> typedFactory = (ObjectIdDiscovererFactory<T>) matched.get(0);
        return typedFactory;
    }

    /**
     * Loads all {@link ObjectIdDiscovererFactory} implementations from both the {@link
     * ObjectIdDiscovererFactory} and the legacy {@link TableDiscovererFactory} SPI service files,
     * de-duplicated by implementation class.
     */
    private static Iterable<ObjectIdDiscovererFactory<?>> loadFactories(ClassLoader loader) {
        Map<String, ObjectIdDiscovererFactory<?>> factories = new LinkedHashMap<>();
        for (ObjectIdDiscovererFactory<?> factory :
                ServiceLoader.load(ObjectIdDiscovererFactory.class, loader)) {
            factories.putIfAbsent(factory.getClass().getName(), factory);
        }
        for (TableDiscovererFactory factory :
                ServiceLoader.load(TableDiscovererFactory.class, loader)) {
            factories.putIfAbsent(factory.getClass().getName(), factory);
        }
        return factories.values();
    }

    private static String normalizeType(String type) {
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("Discoverer factory type must not be empty.");
        }
        return type.trim().toLowerCase(Locale.ROOT);
    }

    private static String formatFactory(
            String type, Class<?> objectIdClass, ObjectIdDiscovererFactory<?> factory) {
        return type + " + " + objectIdClass.getName() + " (" + factory.getClass().getName() + ")";
    }

    /** Default implementation of {@link ObjectIdDiscoverer.Context}. */
    class DefaultObjectIdDiscovererContext implements ObjectIdDiscoverer.Context {
        private final Configuration configuration;
        private final ClassLoader classLoader;

        DefaultObjectIdDiscovererContext(Configuration configuration, ClassLoader classLoader) {
            this.configuration = configuration;
            this.classLoader = classLoader;
        }

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        @Override
        public ClassLoader getUserCodeClassLoader() {
            return classLoader;
        }
    }
}
