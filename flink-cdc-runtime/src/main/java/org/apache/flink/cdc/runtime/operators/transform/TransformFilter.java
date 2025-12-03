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

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The TransformFilter applies to describe the information of the filter row.
 *
 * <p>A filter row contains:
 *
 * <ul>
 *   <li>expression: a string for filter expression split from the user-defined filter.
 *   <li>scriptExpression: a string for filter script expression compiled from the column
 *       expression.
 *   <li>columnNames: a list for recording the name of all columns used by the filter expression.
 * </ul>
 */
public class TransformFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String expression;
    private final String scriptExpression;
    private final List<String> columnNames;
    private final Map<String, String> columnNameMap;

    public TransformFilter(
            String expression,
            String scriptExpression,
            List<String> columnNames,
            Map<String, String> columnNameMap) {
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.columnNames = columnNames;
        this.columnNameMap = columnNameMap;
    }

    public String getExpression() {
        return expression;
    }

    public String getScriptExpression() {
        return scriptExpression;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Map<String, String> getColumnNameMap() {
        return columnNameMap;
    }

    public String getColumnNameMapAsString() {
        return TransformException.prettyPrintColumnNameMap(getColumnNameMap());
    }

    public static Optional<TransformFilter> of(
            String filterExpression, List<UserDefinedFunctionDescriptor> udfDescriptors) {
        if (StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            return Optional.empty();
        }
        List<String> columnNames = TransformParser.parseFilterColumnNameList(filterExpression);
        Map<String, String> columnNameMap = TransformParser.generateColumnNameMap(columnNames);
        String scriptExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        filterExpression, udfDescriptors, columnNameMap);
        return Optional.of(
                new TransformFilter(
                        filterExpression, scriptExpression, columnNames, columnNameMap));
    }

    public boolean isValid() {
        return !columnNames.isEmpty();
    }

    @Override
    public String toString() {
        return "TransformFilter{"
                + "expression='"
                + expression
                + '\''
                + ", scriptExpression='"
                + scriptExpression
                + '\''
                + ", columnNames="
                + columnNames
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
