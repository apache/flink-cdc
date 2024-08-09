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
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.runtime.parser.JaninoCompiler;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.cdc.runtime.parser.TransformParser.DEFAULT_NAMESPACE_NAME;
import static org.apache.flink.cdc.runtime.parser.TransformParser.DEFAULT_SCHEMA_NAME;
import static org.apache.flink.cdc.runtime.parser.TransformParser.DEFAULT_TABLE_NAME;

/** The processor of the transform filter. It processes the data change event of matched table. */
public class TransformFilterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TransformFilterProcessor.class);
    private PostTransformChangeInfo tableInfo;
    private TransformFilter transformFilter;
    private String timezone;
    private TransformExpressionKey transformExpressionKey;
    private final transient List<Object> udfFunctionInstances;
    private transient ExpressionEvaluator expressionEvaluator;

    public TransformFilterProcessor(
            PostTransformChangeInfo tableInfo,
            TransformFilter transformFilter,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances) {
        this.tableInfo = tableInfo;
        this.transformFilter = transformFilter;
        this.timezone = timezone;
        this.transformExpressionKey = generateTransformExpressionKey();
        this.udfFunctionInstances = udfFunctionInstances;
        this.expressionEvaluator =
                TransformExpressionCompiler.compileExpression(
                        transformExpressionKey, udfDescriptors);
    }

    public static TransformFilterProcessor of(
            PostTransformChangeInfo tableInfo,
            TransformFilter transformFilter,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances) {
        return new TransformFilterProcessor(
                tableInfo, transformFilter, timezone, udfDescriptors, udfFunctionInstances);
    }

    public boolean process(BinaryRecordData after, long epochTime) {
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

    private Tuple2<List<String>, List<Class<?>>> generateArguments() {
        List<String> argNames = new ArrayList<>();
        List<Class<?>> argTypes = new ArrayList<>();
        String scriptExpression = transformFilter.getScriptExpression();
        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();
        LinkedHashSet<String> columnNames = new LinkedHashSet<>(transformFilter.getColumnNames());
        for (String columnName : columnNames) {
            for (Column column : columns) {
                if (column.getName().equals(columnName)) {
                    argNames.add(columnName);
                    argTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }
        }
        Stream.of(DEFAULT_NAMESPACE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME)
                .forEach(
                        metadataColumn -> {
                            if (scriptExpression.contains(metadataColumn)
                                    && !argNames.contains(metadataColumn)) {
                                argNames.add(metadataColumn);
                                argTypes.add(String.class);
                            }
                        });
        return Tuple2.of(argNames, argTypes);
    }

    private Object[] generateParams(BinaryRecordData after, long epochTime) {
        List<Object> params = new ArrayList<>();
        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();

        // 1 - Add referenced columns
        Tuple2<List<String>, List<Class<?>>> args = generateArguments();
        RecordData.FieldGetter[] fieldGetters = tableInfo.getPreTransformedFieldGetters();
        for (String columnName : args.f0) {
            switch (columnName) {
                case DEFAULT_NAMESPACE_NAME:
                    params.add(tableInfo.getNamespace());
                    continue;
                case DEFAULT_SCHEMA_NAME:
                    params.add(tableInfo.getSchemaName());
                    continue;
                case DEFAULT_TABLE_NAME:
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

        // 2 - Add time-sensitive function arguments
        params.add(timezone);
        params.add(epochTime);

        // 3 - Add UDF function instances
        params.addAll(udfFunctionInstances);
        return params.toArray();
    }

    private TransformExpressionKey generateTransformExpressionKey() {
        Tuple2<List<String>, List<Class<?>>> args = generateArguments();

        args.f0.add(JaninoCompiler.DEFAULT_TIME_ZONE);
        args.f1.add(String.class);
        args.f0.add(JaninoCompiler.DEFAULT_EPOCH_TIME);
        args.f1.add(Long.class);

        return TransformExpressionKey.of(
                JaninoCompiler.loadSystemFunction(transformFilter.getScriptExpression()),
                args.f0,
                args.f1,
                Boolean.class);
    }
}
