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

package org.apache.flink.cdc.common.lineage;

import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaField;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** A {@link LineageDataset} implementation for CDC source tables. */
public class CDCLineageDataset implements LineageDataset {

    private final String name;
    private final String namespace;
    private final Map<String, String> config;
    @Nullable private final LinkedHashMap<String, String> schema;

    /**
     * Creates a CDC lineage dataset without a schema facet.
     *
     * @param name dataset name, usually a concrete table name
     * @param namespace dataset namespace identifying the source system
     * @param config config facet values for the dataset
     */
    public CDCLineageDataset(String name, String namespace, Map<String, String> config) {
        this(name, namespace, config, null);
    }

    /**
     * Creates a CDC lineage dataset with optional schema metadata.
     *
     * @param name dataset name, usually a concrete table name
     * @param namespace dataset namespace identifying the source system
     * @param config config facet values for the dataset
     * @param schema optional ordered map from field name to field type
     */
    public CDCLineageDataset(
            String name,
            String namespace,
            Map<String, String> config,
            @Nullable LinkedHashMap<String, String> schema) {
        this.name = name;
        this.namespace = namespace;
        this.config = config;
        this.schema = schema;
    }

    /**
     * Returns the dataset name.
     *
     * @return dataset name, usually a concrete table name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Returns the namespace identifying the source system.
     *
     * @return dataset namespace
     */
    @Override
    public String namespace() {
        return namespace;
    }

    /**
     * Returns the dataset facets.
     *
     * <p>Every dataset contains a {@code config} facet. Datasets with non-empty schema metadata
     * also contain a {@code schema} facet.
     *
     * @return map of facet name to lineage dataset facet
     */
    @Override
    public Map<String, LineageDatasetFacet> facets() {
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(
                "config",
                new DatasetConfigFacet() {
                    @Override
                    public String name() {
                        return "config";
                    }

                    @Override
                    public Map<String, String> config() {
                        return config;
                    }
                });
        if (schema != null && !schema.isEmpty()) {
            facets.put(
                    "schema",
                    new DatasetSchemaFacet() {
                        @Override
                        public String name() {
                            return "schema";
                        }

                        @Override
                        public Map<String, DatasetSchemaField<String>> fields() {
                            Map<String, DatasetSchemaField<String>> result = new LinkedHashMap<>();
                            for (Map.Entry<String, String> entry : schema.entrySet()) {
                                String fieldName = entry.getKey();
                                String fieldType = entry.getValue();
                                result.put(
                                        fieldName,
                                        new DatasetSchemaField<String>() {
                                            @Override
                                            public String name() {
                                                return fieldName;
                                            }

                                            @Override
                                            public String type() {
                                                return fieldType;
                                            }
                                        });
                            }
                            return result;
                        }
                    });
        }
        return facets;
    }
}
