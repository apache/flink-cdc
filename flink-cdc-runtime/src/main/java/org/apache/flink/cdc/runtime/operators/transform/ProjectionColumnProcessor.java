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
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
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
import java.util.Map;
import java.util.stream.Stream;

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
    private final SupportedMetadataColumn[] supportedMetadataColumns;
    private final transient List<Object> udfFunctionInstances;
    private transient ExpressionEvaluator expressionEvaluator;

    public ProjectionColumnProcessor(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            final List<Object> udfFunctionInstances,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.tableInfo = tableInfo;
        this.projectionColumn = projectionColumn;
        this.timezone = timezone;
        this.udfDescriptors = udfDescriptors;
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
            SupportedMetadataColumn[] supportedMetadataColumns) {
        return new ProjectionColumnProcessor(
                tableInfo,
                projectionColumn,
                timezone,
                udfDescriptors,
                udfFunctionInstances,
                supportedMetadataColumns);
    }

    public ProjectionColumn getProjectionColumn() {
        return projectionColumn;
    }

    public Object evaluate(
            BinaryRecordData record, long epochTime, String opType, Map<String, String> meta) {
        try {
            return expressionEvaluator.evaluate(generateParams(record, epochTime, opType, meta));
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

    private Object[] generateParams(
            BinaryRecordData record, long epochTime, String opType, Map<String, String> meta) {
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

            boolean foundInMeta = false;
            for (SupportedMetadataColumn supportedMetadataColumn : supportedMetadataColumns) {
                if (supportedMetadataColumn.getName().equals(originalColumnName)) {
                    params.add(supportedMetadataColumn.read(meta));
                    foundInMeta = true;
                    break;
                }
            }
            if (foundInMeta) {
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
            Stream.of(supportedMetadataColumns)
                    .filter(col -> col.getName().equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(columnNameMap.get(col.getName()));
                                paramTypes.add(col.getJavaClass());
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
