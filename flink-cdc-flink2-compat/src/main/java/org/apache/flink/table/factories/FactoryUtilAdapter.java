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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Compatibility adapter for {@link FactoryUtil} in Flink 2.2.
 *
 * <p>In Flink 2.x, FactoryUtil.createTableSource() has been replaced with createDynamicTableSource
 * with different parameters.
 *
 * <p>This adapter provides a factory method that accepts the Flink 1.x parameter order and creates
 * a DynamicTableSource compatible with Flink 2.x.
 */
@Internal
public class FactoryUtilAdapter {

    /**
     * Creates a DynamicTableSource using the Flink 2.x API.
     *
     * @param context the context (can be null)
     * @param objectIdentifier the object identifier
     * @param resolvedCatalogTable the resolved catalog table
     * @param configuration the configuration
     * @param classLoader the class loader
     * @param isStreaming whether it is streaming
     * @return a new DynamicTableSource instance
     */
    public static DynamicTableSource createTableSource(
            Object context,
            ObjectIdentifier objectIdentifier,
            ResolvedCatalogTable resolvedCatalogTable,
            Configuration configuration,
            ClassLoader classLoader,
            boolean isStreaming) {

        // Get the connector option to find the factory
        String connector = resolvedCatalogTable.getOptions().get("connector");
        if (connector == null) {
            throw new IllegalArgumentException("Missing required option 'connector'");
        }

        // Discover the factory
        DynamicTableSourceFactory factory =
                FactoryUtil.discoverFactory(
                        classLoader, DynamicTableSourceFactory.class, connector);

        // Create the source using Flink 2.x API
        Map<String, String> enrichedOptions = new HashMap<>(resolvedCatalogTable.getOptions());
        ReadableConfig readableConfig = configuration;

        return FactoryUtil.createDynamicTableSource(
                factory,
                objectIdentifier,
                resolvedCatalogTable,
                enrichedOptions,
                readableConfig,
                classLoader,
                isStreaming);
    }
}
