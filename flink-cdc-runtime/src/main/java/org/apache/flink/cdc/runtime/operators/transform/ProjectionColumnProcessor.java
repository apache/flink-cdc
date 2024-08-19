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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.runtime.parser.JaninoCompiler;
import org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;

/**
 * The processor of the projection column. It processes the data column and the user-defined
 * computed columns.
 */
public class ProjectionColumnProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectionColumnProcessor.class);

    private PostTransformChangeInfo tableInfo;
    private ProjectionColumn projectionColumn;
    private String timezone;
    private TransformExpressionKey transformExpressionKey;
    private final List<UserDefinedFunctionDescriptor> udfDescriptors;
    private final transient List<Object> udfFunctionInstances;
    private transient ExpressionEvaluator expressionEvaluator;

    public ProjectionColumnProcessor(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            final List<Object> udfFunctionInstances) {
        this.tableInfo = tableInfo;
        this.projectionColumn = projectionColumn;
        this.timezone = timezone;
        this.udfDescriptors = udfDescriptors;
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
            List<Object> udfFunctionInstances) {
        return new ProjectionColumnProcessor(
                tableInfo, projectionColumn, timezone, udfDescriptors, udfFunctionInstances);
    }

    public ProjectionColumn getProjectionColumn() {
        return projectionColumn;
    }

    public Object evaluate(BinaryRecordData record, long epochTime, String opType) {
        try {
            return expressionEvaluator.evaluate(generateParams(record, epochTime, opType));
        } catch (InvocationTargetException e) {
            LOG.error(
                    "Table:{} column:{} projection:{} execute failed. {}",
                    tableInfo.getName(),
                    projectionColumn.getColumnName(),
                    projectionColumn.getScriptExpression(),
                    e);
            throw new RuntimeException(e);
        }
    }

    private Object[] generateParams(BinaryRecordData record, long epochTime, String opType) {
        List<Object> params = new ArrayList<>();
        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();

        // 1 - Add referenced columns
        RecordData.FieldGetter[] fieldGetters = tableInfo.getPreTransformedFieldGetters();
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String originalColumnName : originalColumnNames) {
            switch (originalColumnName) {
                case MetadataColumns.DEFAULT_NAMESPACE_NAME:
                    params.add(tableInfo.getNamespace());
                    continue;
                case MetadataColumns.DEFAULT_SCHEMA_NAME:
                    params.add(tableInfo.getSchemaName());
                    continue;
                case MetadataColumns.DEFAULT_TABLE_NAME:
                    params.add(tableInfo.getTableName());
                    continue;
                case MetadataColumns.DEFAULT_DATA_EVENT_TYPE:
                    params.add(opType);
                    continue;
            }

            boolean argumentFound = false;
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                if (column.getName().equals(originalColumnName)) {
                    params.add(
                            DataTypeConverter.convertToOriginal(
                                    fieldGetters[i].getFieldOrNull(record), column.getType()));
                    argumentFound = true;
                    break;
                }
            }
            if (!argumentFound) {
                throw new IllegalArgumentException(
                        "Failed to evaluate argument " + originalColumnName);
            }
        }

        // 2 - Add time-sensitive function arguments
        params.add(timezone);
        params.add(epochTime);

        // 3 - Add UDF function instances
        params.addAll(udfFunctionInstances);
        return params.toArray();
    }

    private TransformExpressionKey generateTransformExpressionKey() {
        List<String> argumentNames = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();
        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();
        String scriptExpression = projectionColumn.getScriptExpression();
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String originalColumnName : originalColumnNames) {
            for (Column column : columns) {
                if (column.getName().equals(originalColumnName)) {
                    argumentNames.add(originalColumnName);
                    paramTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }
        }

        for (String originalColumnName : originalColumnNames) {
            METADATA_COLUMNS.stream()
                    .filter(col -> col.f0.equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(col.f0);
                                paramTypes.add(col.f2);
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
                DataTypeConverter.convertOriginalClass(projectionColumn.getDataType()));
    }
}
