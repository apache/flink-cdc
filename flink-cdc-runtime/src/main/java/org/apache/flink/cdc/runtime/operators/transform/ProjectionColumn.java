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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The ProjectionColumn applies to describe the information of the transformation column. If it only
 * has column info, it describes the data column. If it has column info and expression info, it
 * describes the user-defined computed columns.
 *
 * <p>A projection column contains:
 *
 * <ul>
 *   <li>column: column information parsed from projection.
 *   <li>expression: a string for column expression split from the user-defined projection.
 *   <li>scriptExpression: a string for column script expression compiled from the column
 *       expression.
 *   <li>originalColumnNames: a list for recording the name of all columns used by the column
 *       expression.
 * </ul>
 */
public class ProjectionColumn implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Column column;
    private final String expression;
    private final String scriptExpression;
    private final List<String> originalColumnNames;
    private TransformExpressionKey transformExpressionKey;

    public ProjectionColumn(
            Column column,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames) {
        this.column = column;
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.originalColumnNames = originalColumnNames;
    }

    public ProjectionColumn copy() {
        return new ProjectionColumn(
                column.copy(column.getName()),
                expression,
                scriptExpression,
                new ArrayList<>(originalColumnNames));
    }

    public Column getColumn() {
        return column;
    }

    public String getColumnName() {
        return column.getName();
    }

    public DataType getDataType() {
        return column.getType();
    }

    public String getScriptExpression() {
        return scriptExpression;
    }

    public List<String> getOriginalColumnNames() {
        return originalColumnNames;
    }

    public void setTransformExpressionKey(TransformExpressionKey transformExpressionKey) {
        this.transformExpressionKey = transformExpressionKey;
    }

    public boolean isValidTransformedProjectionColumn() {
        return !StringUtils.isNullOrWhitespaceOnly(scriptExpression);
    }

    public static ProjectionColumn of(String columnName, DataType dataType) {
        return new ProjectionColumn(Column.physicalColumn(columnName, dataType), null, null, null);
    }

    public static ProjectionColumn of(
            String columnName,
            DataType dataType,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames) {
        return new ProjectionColumn(
                Column.physicalColumn(columnName, dataType),
                expression,
                scriptExpression,
                originalColumnNames);
    }

    @Override
    public String toString() {
        return "ProjectionColumn{"
                + "column="
                + column
                + ", expression='"
                + expression
                + '\''
                + ", scriptExpression='"
                + scriptExpression
                + '\''
                + ", originalColumnNames="
                + originalColumnNames
                + ", transformExpressionKey="
                + transformExpressionKey
                + '}';
    }
}
