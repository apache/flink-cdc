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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.schema.Column;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies {@link SchemaColumnCaseFormat} to post-projection columns and resolves primary /
 * partition keys consistently with that naming.
 */
final class SchemaColumnCaseFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaColumnCaseFormatter.class);

    private SchemaColumnCaseFormatter() {}

    static List<Column> applyCaseFormatToColumns(
            List<ProjectionColumn> projectionColumns, SchemaColumnCaseFormat caseFormat) {
        if (caseFormat == SchemaColumnCaseFormat.AS_IS) {
            return projectionColumns.stream()
                    .map(ProjectionColumn::getColumn)
                    .collect(Collectors.toList());
        }
        return projectionColumns.stream()
                .map(pc -> applyCaseFormatToOneColumn(pc, caseFormat))
                .collect(Collectors.toList());
    }

    private static Column applyCaseFormatToOneColumn(
            ProjectionColumn pc, SchemaColumnCaseFormat caseFormat) {
        Column column = pc.getColumn();
        String columnName = column.getName();
        List<String> originalNames = pc.getOriginalColumnNames();

        if (originalNames.size() > 1) {
            return column;
        }
        if (originalNames.size() == 1 && !originalNames.get(0).equals(columnName)) {
            return column;
        }
        String newName = convertCase(columnName, caseFormat);
        return newName.equals(columnName) ? column : column.copy(newName);
    }

    static String convertCase(String name, SchemaColumnCaseFormat caseFormat) {
        if (name == null) {
            return null;
        }
        switch (caseFormat) {
            case UPPER:
                return name.toUpperCase(Locale.ROOT);
            case LOWER:
                return name.toLowerCase(Locale.ROOT);
            case AS_IS:
            default:
                return name;
        }
    }

    /**
     * Source physical column → final post-schema column name, including case-format on simple
     * forwards. Only {@link ProjectionColumn#isSimpleColumnRenameOrForward()} entries participate;
     * ambiguous sources (appearing more than once as single-source) are dropped.
     */
    static Map<String, String> buildProvableLineageMap(
            List<ProjectionColumn> projectionColumns, SchemaColumnCaseFormat caseFormat) {
        Map<String, Integer> sourceOccurrenceCount = new HashMap<>();
        for (ProjectionColumn pc : projectionColumns) {
            List<String> originalNames = pc.getOriginalColumnNames();
            if (originalNames.size() == 1) {
                sourceOccurrenceCount.merge(originalNames.get(0), 1, Integer::sum);
            }
        }

        Map<String, String> candidateMap = new HashMap<>();
        for (ProjectionColumn pc : projectionColumns) {
            if (!pc.isSimpleColumnRenameOrForward()) {
                continue;
            }
            List<String> originalNames = pc.getOriginalColumnNames();
            if (originalNames.size() != 1) {
                continue;
            }
            String sourceColumn = originalNames.get(0);
            String targetColumn = pc.getColumnName();
            boolean aliased = !sourceColumn.equals(targetColumn);
            String finalTarget = aliased ? targetColumn : convertCase(targetColumn, caseFormat);
            candidateMap.put(sourceColumn, finalTarget);
        }
        return candidateMap.entrySet().stream()
                .filter(e -> sourceOccurrenceCount.get(e.getKey()) == 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static List<String> resolveProjectedKeys(
            List<String> preSchemaKeys,
            List<String> explicitPostKeys,
            Map<String, String> lineageMap,
            Set<String> projectedColumnNames,
            TableId tableId,
            String explicitKeyKindLabel,
            SchemaColumnCaseFormat caseFormat) {

        if (!explicitPostKeys.isEmpty()) {
            LinkedHashSet<String> resolvedKeys = new LinkedHashSet<>();
            List<String> caseMatchedOriginalKeys = new ArrayList<>();
            List<String> unresolvedKeys = new ArrayList<>();
            for (String explicitKey : explicitPostKeys) {
                if (projectedColumnNames.contains(explicitKey)) {
                    resolvedKeys.add(explicitKey);
                    continue;
                }

                String convertedKey = convertCase(explicitKey, caseFormat);
                if (projectedColumnNames.contains(convertedKey)) {
                    resolvedKeys.add(convertedKey);
                    caseMatchedOriginalKeys.add(explicitKey);
                    continue;
                }

                unresolvedKeys.add(explicitKey);
            }

            if (unresolvedKeys.isEmpty()) {
                if (!caseMatchedOriginalKeys.isEmpty()) {
                    List<String> resolvedByCase =
                            explicitPostKeys.stream()
                                    .filter(caseMatchedOriginalKeys::contains)
                                    .map(k -> convertCase(k, caseFormat))
                                    .collect(Collectors.toList());
                    LOG.warn(
                            "Explicit transform {} keys {} do not match post-projection columns for table {}. "
                                    + "They match after applying {}: {}. Using {}. Prefer projected names.",
                            explicitKeyKindLabel,
                            explicitPostKeys,
                            tableId.identifier(),
                            caseFormat,
                            resolvedByCase,
                            resolvedKeys);
                }
                return resolvedKeys.stream().collect(Collectors.toList());
            }

            LOG.warn(
                    "Explicit transform {} key column(s) {} are not found in the post-projection schema for table {}. "
                            + "Post-projection columns: {}. Falling back to automatic key remapping from the upstream schema.",
                    explicitKeyKindLabel,
                    unresolvedKeys,
                    tableId.identifier(),
                    projectedColumnNames);
        }
        return remapKeyNames(preSchemaKeys, lineageMap, projectedColumnNames);
    }

    private static List<String> remapKeyNames(
            List<String> originalKeys,
            Map<String, String> lineageMap,
            Set<String> projectedColumnNames) {
        return originalKeys.stream()
                .map(key -> lineageMap.getOrDefault(key, key))
                .filter(projectedColumnNames::contains)
                .collect(Collectors.toList());
    }
}
