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
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/** The processor of the transform filter. It processes the data change event of matched table. */
public class TransformFilterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TransformFilterProcessor.class);
    private TableInfo tableInfo;
    private TransformFilter transformFilter;
    private String timezone;
    private TransformExpressionKey transformExpressionKey;

    public TransformFilterProcessor(
            TableInfo tableInfo, TransformFilter transformFilter, String timezone) {
        this.tableInfo = tableInfo;
        this.transformFilter = transformFilter;
        this.timezone = timezone;
        transformExpressionKey = generateTransformExpressionKey();
    }

    public static TransformFilterProcessor of(
            TableInfo tableInfo, TransformFilter transformFilter, String timezone) {
        return new TransformFilterProcessor(tableInfo, transformFilter, timezone);
    }

    public boolean process(BinaryRecordData after, long epochTime) {
        ExpressionEvaluator expressionEvaluator =
                TransformExpressionCompiler.compileExpression(transformExpressionKey);
        try {
            return (Boolean) expressionEvaluator.evaluate(generateParams(after, epochTime));
        } catch (InvocationTargetException e) {
            LOG.error(
                    "Table:{} filter:{} execute failed. {}",
                    tableInfo.getName(),
                    transformFilter.getExpression(),
                    e);
            throw new RuntimeException(e);
        }
    }

    private Object[] generateParams(BinaryRecordData after, long epochTime) {
        List<Object> params = new ArrayList<>();
        List<Column> columns = tableInfo.getSchema().getColumns();
        RecordData.FieldGetter[] fieldGetters = tableInfo.getFieldGetters();
        for (String columnName : transformFilter.getColumnNames()) {
            if (columnName.equals(TransformParser.DEFAULT_NAMESPACE_NAME)) {
                params.add(tableInfo.getNamespace());
                continue;
            }
            if (columnName.equals(TransformParser.DEFAULT_SCHEMA_NAME)) {
                params.add(tableInfo.getSchemaName());
                continue;
            }
            if (columnName.equals(TransformParser.DEFAULT_TABLE_NAME)) {
                params.add(tableInfo.getTableName());
                continue;
            }
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                if (column.getName().equals(columnName)) {
                    params.add(
                            DataTypeConverter.convertToOriginal(
                                    fieldGetters[i].getFieldOrNull(after), column.getType()));
                    break;
                }
            }
        }
        params.add(timezone);
        params.add(epochTime);
        return params.toArray();
    }

    private TransformExpressionKey generateTransformExpressionKey() {
        List<String> argumentNames = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();
        List<Column> columns = tableInfo.getSchema().getColumns();
        String scriptExpression = transformFilter.getScriptExpression();
        List<String> columnNames = transformFilter.getColumnNames();
        for (String columnName : columnNames) {
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                if (column.getName().equals(columnName)) {
                    argumentNames.add(columnName);
                    paramTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }
        }
        if (scriptExpression.contains(TransformParser.DEFAULT_NAMESPACE_NAME)
                && !argumentNames.contains(TransformParser.DEFAULT_NAMESPACE_NAME)) {
            argumentNames.add(TransformParser.DEFAULT_NAMESPACE_NAME);
            paramTypes.add(String.class);
        }

        if (scriptExpression.contains(TransformParser.DEFAULT_SCHEMA_NAME)
                && !argumentNames.contains(TransformParser.DEFAULT_SCHEMA_NAME)) {
            argumentNames.add(TransformParser.DEFAULT_SCHEMA_NAME);
            paramTypes.add(String.class);
        }

        if (scriptExpression.contains(TransformParser.DEFAULT_TABLE_NAME)
                && !argumentNames.contains(TransformParser.DEFAULT_TABLE_NAME)) {
            argumentNames.add(TransformParser.DEFAULT_TABLE_NAME);
            paramTypes.add(String.class);
        }

        argumentNames.add(JaninoCompiler.DEFAULT_TIME_ZONE);
        paramTypes.add(String.class);
        argumentNames.add(JaninoCompiler.DEFAULT_EPOCH_TIME);
        paramTypes.add(Long.class);

        return TransformExpressionKey.of(
                JaninoCompiler.loadSystemFunction(scriptExpression),
                argumentNames,
                paramTypes,
                Boolean.class);
    }
}
