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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utilities for building lineage vertices from CDC source metadata. */
public class LineageUtils {

    /**
     * Builds the dataset namespace for a CDC source.
     *
     * <p>The namespace identifies the source system instance, for example {@code
     * mysql://localhost:3306}.
     *
     * @param type CDC source type, such as {@code mysql}
     * @param hostname source host name
     * @param port source port
     * @return namespace used by lineage datasets
     */
    public static String getNamespace(String type, String hostname, int port) {
        return type + "://" + hostname + ":" + port;
    }

    /**
     * Builds the config facet values shared by all datasets for a CDC source.
     *
     * @param type CDC source type, such as {@code mysql}
     * @return config facet map for lineage datasets
     */
    private static Map<String, String> buildConfigMap(String type) {
        Map<String, String> config = new HashMap<>();
        config.put("type", type + "-cdc");
        return config;
    }

    /**
     * Builds a source lineage vertex without schema facets.
     *
     * @param type CDC source type, such as {@code mysql}
     * @param hostname source host name
     * @param port source port
     * @param isBounded whether the source is bounded
     * @param tableList concrete source table names; if empty, a connector-level dataset is used
     * @return source lineage vertex for the CDC source
     */
    public static SourceLineageVertex sourceLineageVertex(
            String type, String hostname, int port, boolean isBounded, List<String> tableList) {
        return sourceLineageVertex(type, hostname, port, isBounded, tableList, null);
    }

    /**
     * Builds a source lineage vertex for a CDC source.
     *
     * <p>When {@code tableList} is not empty, each table becomes one lineage dataset. When it is
     * empty, a single connector-level dataset is returned. If schemas are provided for a table,
     * they are attached as schema facets on that table dataset.
     *
     * @param type CDC source type, such as {@code mysql}
     * @param hostname source host name
     * @param port source port
     * @param isBounded whether the source is bounded
     * @param tableList concrete source table names
     * @param tableSchemas optional map from table name to ordered field name/type pairs
     * @return source lineage vertex for the CDC source
     */
    public static SourceLineageVertex sourceLineageVertex(
            String type,
            String hostname,
            int port,
            boolean isBounded,
            List<String> tableList,
            Map<String, LinkedHashMap<String, String>> tableSchemas) {
        String namespace = getNamespace(type, hostname, port);
        Boundedness boundedness =
                isBounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;

        Map<String, String> config = buildConfigMap(type);

        List<LineageDataset> datasets;
        if (tableList != null && !tableList.isEmpty()) {
            datasets =
                    tableList.stream()
                            .map(
                                    table -> {
                                        LinkedHashMap<String, String> schema =
                                                tableSchemas != null
                                                        ? tableSchemas.get(table)
                                                        : null;
                                        return (LineageDataset)
                                                new CDCLineageDataset(
                                                        table, namespace, config, schema);
                                    })
                            .collect(Collectors.toList());
        } else {
            datasets = Collections.singletonList(new CDCLineageDataset(type, namespace, config));
        }

        return new CDCSourceLineageVertex(boundedness, datasets);
    }
}
