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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.parser.JaninoCompiler;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.runtime.operators.transform.TransformContext.lookupObjectByName;
import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;

/** The processor of the transform filter. It processes the data change event of matched table. */
public class TransformFilterProcessor {

    private final boolean isNoOp;
    private final PostTransformChangeInfo tableInfo;
    private final TransformFilter transformFilter;
    private final String timezone;
    private final List<Object> udfFunctionInstances;
    private final Map<String, SupportedMetadataColumn> supportedMetadataColumns;

    private final TransformExpressionKey transformExpressionKey;
    private final ExpressionEvaluator expressionEvaluator;

    protected TransformFilterProcessor(
            boolean isNoOp,
            PostTransformChangeInfo tableInfo,
            TransformFilter transformFilter,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns) {
        this.isNoOp = isNoOp;
        this.tableInfo = tableInfo;
        this.transformFilter = transformFilter;
        this.timezone = timezone;
        this.udfFunctionInstances = udfFunctionInstances;
        this.supportedMetadataColumns = supportedMetadataColumns;

        if (isNoOp) {
            this.transformExpressionKey = null;
            this.expressionEvaluator = null;
        } else {
            this.transformExpressionKey = generateTransformExpressionKey();
            this.expressionEvaluator =
                    TransformExpressionCompiler.compileExpression(
                            transformExpressionKey, udfDescriptors);
        }
    }

    public static TransformFilterProcessor ofNoOp() {
        return new TransformFilterProcessor(true, null, null, null, null, null, null);
    }

    public static TransformFilterProcessor of(
            PostTransformChangeInfo tableInfo,
            TransformFilter transformFilter,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        Map<String, SupportedMetadataColumn> supportedMetadataColumnsMap = new HashMap<>();
        for (SupportedMetadataColumn supportedMetadataColumn : supportedMetadataColumns) {
            supportedMetadataColumnsMap.put(
                    supportedMetadataColumn.getName(), supportedMetadataColumn);
        }
        return new TransformFilterProcessor(
                false,
                tableInfo,
                transformFilter,
                timezone,
                udfDescriptors,
                udfFunctionInstances,
                supportedMetadataColumnsMap);
    }

    public boolean test(Object[] preRow, Object[] postRow, TransformContext context) {
        if (isNoOp) {
            return true;
        }

        try {
            return (Boolean) expressionEvaluator.evaluate(generateParams(preRow, postRow, context));
        } catch (InvocationTargetException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to evaluate filtering expression `%s` for table `%s`.\n"
                                    + "\tColumn name map: {%s}",
                            transformFilter.getScriptExpression(),
                            tableInfo.getName(),
                            transformFilter.getColumnNameMapAsString()),
                    e);
        }
    }

    private Tuple2<List<String>, List<Class<?>>> generateArguments(boolean mapColumnNames) {
        List<String> rawArgNames = new ArrayList<>();
        List<String> argNames = new ArrayList<>();
        List<Class<?>> argTypes = new ArrayList<>();
        String expression = transformFilter.getExpression();

        // Post-transformed columns comes in priority
        List<Column> columns = new ArrayList<>(tableInfo.getPostTransformedSchema().getColumns());
        {
            Set<String> existingColumnNames =
                    new HashSet<>(tableInfo.getPostTransformedSchema().getColumnNames());
            tableInfo.getPreTransformedSchema().getColumns().stream()
                    .filter(col -> !existingColumnNames.contains(col.getName()))
                    .forEach(columns::add);
        }

        Map<String, String> columnNameMap = transformFilter.getColumnNameMap();
        LinkedHashSet<String> columnNames = new LinkedHashSet<>(transformFilter.getColumnNames());
        for (String columnName : columnNames) {
            for (Column column : columns) {
                if (column.getName().equals(columnName)) {
                    rawArgNames.add(columnName);
                    argNames.add(mapColumnNames ? columnNameMap.get(columnName) : columnName);
                    argTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }
        }

        METADATA_COLUMNS.forEach(
                col -> {
                    if (expression.contains(col.f0) && !rawArgNames.contains(col.f0)) {
                        rawArgNames.add(col.f0);
                        argNames.add(mapColumnNames ? columnNameMap.get(col.f0) : col.f0);
                        argTypes.add(col.f2);
                    }
                });

        supportedMetadataColumns
                .keySet()
                .forEach(
                        colName -> {
                            if (expression.contains(colName) && !rawArgNames.contains(colName)) {
                                rawArgNames.add(colName);
                                argNames.add(mapColumnNames ? columnNameMap.get(colName) : colName);
                                argTypes.add(supportedMetadataColumns.get(colName).getJavaClass());
                            }
                        });
        return Tuple2.of(argNames, argTypes);
    }

    private Object[] generateParams(Object[] preRow, Object[] postRow, TransformContext context) {
        List<Object> params = new ArrayList<>();

        // 1 - Add referenced columns
        Tuple2<List<String>, List<Class<?>>> args = generateArguments(false);
        for (String columnName : args.f0) {
            params.add(
                    lookupObjectByName(
                            columnName,
                            tableInfo,
                            supportedMetadataColumns,
                            preRow,
                            postRow,
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
        Tuple2<List<String>, List<Class<?>>> args = generateArguments(true);

        args.f0.add(JaninoCompiler.DEFAULT_TIME_ZONE);
        args.f1.add(String.class);
        args.f0.add(JaninoCompiler.DEFAULT_EPOCH_TIME);
        args.f1.add(Long.class);

        return TransformExpressionKey.of(
                JaninoCompiler.loadSystemFunction(transformFilter.getScriptExpression()),
                args.f0,
                args.f1,
                Boolean.class,
                transformFilter.getColumnNameMap());
    }
}
