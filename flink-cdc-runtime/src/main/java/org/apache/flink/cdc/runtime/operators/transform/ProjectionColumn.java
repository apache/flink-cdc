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
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final Map<String, String> columnNameMap;

    /**
     * When true, this projection has provable 1:1 lineage from a single upstream physical column
     * (plain forward, simple rename, or key-preserving CAST in a future parser revision). Used for
     * primary/partition key remapping with {@code column-name-case}.
     */
    private final boolean provablePhysicalKeyLineage;

    public ProjectionColumn(
            Column column,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames,
            Map<String, String> columnNameMap) {
        this(column, expression, scriptExpression, originalColumnNames, columnNameMap, false);
    }

    public ProjectionColumn(
            Column column,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames,
            Map<String, String> columnNameMap,
            boolean provablePhysicalKeyLineage) {
        this.column = column;
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.originalColumnNames = originalColumnNames;
        this.columnNameMap = columnNameMap;
        this.provablePhysicalKeyLineage = provablePhysicalKeyLineage;
    }

    public ProjectionColumn copy() {
        return new ProjectionColumn(
                column.copy(column.getName()),
                expression,
                scriptExpression,
                new ArrayList<>(originalColumnNames),
                new HashMap<>(columnNameMap),
                provablePhysicalKeyLineage);
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

    public String getExpression() {
        return expression;
    }

    public String getScriptExpression() {
        return scriptExpression;
    }

    public List<String> getOriginalColumnNames() {
        return originalColumnNames;
    }

    public Map<String, String> getColumnNameMap() {
        return columnNameMap;
    }

    public String getColumnNameMapAsString() {
        return TransformException.prettyPrintColumnNameMap(getColumnNameMap());
    }

    public boolean isValidTransformedProjectionColumn() {
        return !StringUtils.isNullOrWhitespaceOnly(scriptExpression);
    }

    /**
     * Plain forward, simple identifier rename, or key-preserving single-column CAST (when marked by
     * the parser). Excludes arbitrary expressions and metadata columns built as calculated
     * projections.
     */
    public boolean isSimpleColumnRenameOrForward() {
        return provablePhysicalKeyLineage;
    }

    /**
     * This projection is created with a plain column name. <br>
     * Just like column {@code id} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * Comments and default expressions will be intact.
     */
    public static ProjectionColumn ofForwarded(Column column, String mappedColumnName) {
        String name = column.getName();
        Map<String, String> columnNameMap = Collections.singletonMap(name, mappedColumnName);
        return new ProjectionColumn(
                column,
                name,
                mappedColumnName,
                Collections.singletonList(name),
                columnNameMap,
                true);
    }

    /**
     * This projection is created with a simple $id$ AS $new_id$ expression. <br>
     * Just like column {@code new_name} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * Comments and default expressions will be intact.
     */
    public static ProjectionColumn ofAliased(
            Column column, String newName, String mappedColumnName) {
        String originalName = column.getName();
        Map<String, String> columnNameMap =
                Collections.singletonMap(originalName, mappedColumnName);
        return new ProjectionColumn(
                column.copy(newName),
                originalName,
                mappedColumnName,
                Collections.singletonList(originalName),
                columnNameMap,
                true);
    }

    /**
     * This projection is created with a complex calculation expression. <br>
     * Just like column {@code new_age} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * No comments nor default expressions will be kept.
     */
    public static ProjectionColumn ofCalculated(
            String columnName,
            DataType dataType,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames,
            Map<String, String> columnNameMap) {
        return new ProjectionColumn(
                Column.physicalColumn(columnName, dataType),
                expression,
                scriptExpression,
                originalColumnNames,
                columnNameMap,
                false);
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
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
