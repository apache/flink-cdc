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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.parser.JaninoCompiler;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.runtime.operators.transform.TransformContext.lookupObjectByName;
import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;

/**
 * The processor of the projection column. It processes the data column and the user-defined
 * computed columns.
 */
public class ProjectionColumnProcessor {

    private final PostTransformChangeInfo tableInfo;
    private final ProjectionColumn projectionColumn;
    private final String timezone;
    private final TransformExpressionKey transformExpressionKey;
    private final Map<String, SupportedMetadataColumn> supportedMetadataColumns;
    private final List<Object> udfFunctionInstances;
    private final ExpressionEvaluator expressionEvaluator;

    public ProjectionColumnProcessor(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            final List<Object> udfFunctionInstances,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns) {
        this.tableInfo = tableInfo;
        this.projectionColumn = projectionColumn;
        this.timezone = timezone;
        this.supportedMetadataColumns = supportedMetadataColumns;
        this.transformExpressionKey = generateTransformExpressionKey();
        this.expressionEvaluator =
                TransformExpressionCompiler.compileExpression(
                        transformExpressionKey, udfDescriptors);
        this.udfFunctionInstances = udfFunctionInstances;
    }

    public static ProjectionColumnProcessor of(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns) {
        return new ProjectionColumnProcessor(
                tableInfo,
                projectionColumn,
                timezone,
                udfDescriptors,
                udfFunctionInstances,
                supportedMetadataColumns);
    }

    public Object evaluate(Object[] rowData, TransformContext context) {
        try {
            Object[] params = generateParams(rowData, context);
            return expressionEvaluator.evaluate(params);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to evaluate projection expression `%s` for column `%s` in table `%s`.\n"
                                    + "\tColumn name map: {%s}",
                            projectionColumn.getScriptExpression(),
                            projectionColumn.getColumnName(),
                            tableInfo.getName(),
                            projectionColumn.getColumnNameMapAsString()),
                    e);
        }
    }

    private Object[] generateParams(Object[] rowData, TransformContext context) {
        List<Object> params = new ArrayList<>();

        // 1 - Add referenced columns
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String columnName : originalColumnNames) {
            params.add(
                    lookupObjectByName(
                            columnName,
                            tableInfo,
                            supportedMetadataColumns,
                            rowData,
                            null,
                            context));
        }

        // 2 - Add time-sensitive function arguments
        params.add(timezone);
        params.add(context.epochTime);

        // 3 - Add UDF function instances
        params.addAll(udfFunctionInstances);
        return params.toArray();
    }

    private TransformExpressionKey generateTransformExpressionKey() {
        List<String> argumentNames = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();

        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();
        String scriptExpression = projectionColumn.getScriptExpression();
        Map<String, String> columnNameMap = projectionColumn.getColumnNameMap();
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String originalColumnName : originalColumnNames) {
            for (Column column : columns) {
                if (column.getName().equals(originalColumnName)) {
                    argumentNames.add(columnNameMap.get(originalColumnName));
                    paramTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }

            METADATA_COLUMNS.stream()
                    .filter(col -> col.f0.equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(columnNameMap.get(col.f0));
                                paramTypes.add(col.f2);
                            });

            supportedMetadataColumns.entrySet().stream()
                    .filter(col -> col.getValue().getName().equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(columnNameMap.get(col.getValue().getName()));
                                paramTypes.add(col.getValue().getJavaClass());
                            });
        }

        argumentNames.add(JaninoCompiler.DEFAULT_TIME_ZONE);
        paramTypes.add(String.class);

        argumentNames.add(JaninoCompiler.DEFAULT_EPOCH_TIME);
        paramTypes.add(Long.class);

        return TransformExpressionKey.of(
                JaninoCompiler.loadSystemFunction(scriptExpression),
                argumentNames,
                paramTypes,
                DataTypeConverter.convertOriginalClass(projectionColumn.getDataType()),
                columnNameMap);
    }
}
