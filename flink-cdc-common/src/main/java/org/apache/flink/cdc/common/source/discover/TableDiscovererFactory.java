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

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * SPI factory that creates a {@link TableDiscoverer}. Implementations are discovered at runtime via
 * Java's {@link ServiceLoader} mechanism. To register a new discoverer type, add a line with the
 * fully-qualified factory class name to:
 *
 * <pre>{@code
 * META-INF/services/org.apache.flink.cdc.common.source.discover.TableDiscovererFactory
 * }</pre>
 *
 * <p>The discoverer type is selected by the user via the {@code table.discoverer.type} option,
 * which is matched (case-insensitively) against {@link #identifier()}.
 */
@PublicEvolving
public interface TableDiscovererFactory {

    /**
     * The unique identifier of this discoverer factory, used to match against the {@code
     * table.discoverer.type} option. Must be lowercase, e.g. {@code "jdbc"} or {@code
     * "fluss-default"}.
     */
    String identifier();

    /**
     * Creates a new {@link TableDiscoverer} instance. The returned discoverer is not yet
     * initialized; the caller must invoke {@link TableDiscoverer#open(TableDiscoverer.Context)}
     * before calling {@link TableDiscoverer#discover()}.
     */
    TableDiscoverer createDiscoverer();

    /**
     * Creates a {@link TableDiscoverer.Context} with the given configuration and class loader. This
     * is a convenience factory method so that callers do not need to implement the {@link
     * TableDiscoverer.Context} interface themselves.
     *
     * @param configuration The full connector configuration.
     * @param classLoader The user code class loader.
     * @return A new {@link TableDiscoverer.Context} instance.
     */
    static TableDiscoverer.Context createContext(
            Configuration configuration, ClassLoader classLoader) {
        return new DefaultDiscovererContext(configuration, classLoader);
    }

    /**
     * Utility method that discovers a {@link TableDiscovererFactory} via SPI whose {@link
     * #identifier()} matches the given {@code type}, and delegates discoverer creation to it.
     *
     * @param type The discoverer type identifier (e.g. "jdbc", "fluss-default").
     * @param classLoader The class loader used for SPI discovery.
     * @return A new, uninitialized {@link TableDiscoverer}.
     * @throws IllegalArgumentException if no factory matches the given type.
     * @throws IllegalStateException if multiple factories share the same identifier.
     */
    static TableDiscoverer createDiscoverer(String type, ClassLoader classLoader) {
        ClassLoader loader =
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        ServiceLoader<TableDiscovererFactory> serviceLoader =
                ServiceLoader.load(TableDiscovererFactory.class, loader);

        TableDiscovererFactory matched = null;
        Set<String> known = new HashSet<>();
        for (TableDiscovererFactory factory : serviceLoader) {
            known.add(factory.identifier());
            if (factory.identifier().equalsIgnoreCase(type)) {
                if (matched != null) {
                    throw new IllegalStateException(
                            "Multiple TableDiscovererFactory implementations found for identifier '"
                                    + type
                                    + "': "
                                    + matched.getClass().getName()
                                    + " and "
                                    + factory.getClass().getName());
                }
                matched = factory;
            }
        }
        if (matched == null) {
            throw new IllegalArgumentException(
                    "Unsupported 'table.discoverer.type' value: '"
                            + type
                            + "'. Available discoverer types: "
                            + known
                            + ".");
        }
        return matched.createDiscoverer();
    }

    /** Default implementation of {@link TableDiscoverer.Context}. */
    class DefaultDiscovererContext implements TableDiscoverer.Context {
        private final Configuration configuration;
        private final ClassLoader classLoader;

        DefaultDiscovererContext(Configuration configuration, ClassLoader classLoader) {
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
