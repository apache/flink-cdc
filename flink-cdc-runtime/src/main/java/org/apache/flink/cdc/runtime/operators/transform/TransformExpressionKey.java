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

import org.apache.flink.cdc.runtime.parser.JaninoCompiler;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The key applies to describe the information of the transformation expression.
 *
 * <p>A transform expression key contains:
 *
 * <ul>
 *   <li>originalExpression: a string for the original transformation expression input by users.
 *   <li>expression: a string for the compiled transformation expression.
 *   <li>argumentNames: a list for the argument names in expression.
 *   <li>argumentClasses: a list for the argument classes in expression.
 *   <li>returnClass: a class for the return class in expression
 *   <li>columnNameMap: a map whose key is the original column name and value is the mapped column
 *       name
 * </ul>
 */
public class TransformExpressionKey implements Serializable {
    private static final long serialVersionUID = 1L;
    @Nullable private final String originalExpression;
    private final String expression;
    private final List<String> argumentNames;
    private final List<Class<?>> argumentClasses;
    private final Class<?> returnClass;
    private final Map<String, String> columnNameMap;

    private TransformExpressionKey(
            @Nullable String originalExpression,
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass,
            Map<String, String> columnNameMap) {
        this.originalExpression = originalExpression;
        this.expression = expression;
        this.argumentNames = argumentNames;
        this.argumentClasses = argumentClasses;
        this.returnClass = returnClass;
        this.columnNameMap = columnNameMap;
    }

    @Nullable
    public String getOriginalExpression() {
        return originalExpression;
    }

    public String getExpression() {
        return expression;
    }

    public String getFullExpression() {
        return JaninoCompiler.loadSystemFunction(expression);
    }

    public List<String> getArgumentNames() {
        return Collections.unmodifiableList(argumentNames);
    }

    public List<Class<?>> getArgumentClasses() {
        return Collections.unmodifiableList(argumentClasses);
    }

    public Class<?> getReturnClass() {
        return returnClass;
    }

    public Map<String, String> getColumnNameMap() {
        return Collections.unmodifiableMap(columnNameMap);
    }

    public static TransformExpressionKey of(
            @Nullable String originalExpression,
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass,
            Map<String, String> columnNameMap) {
        return new TransformExpressionKey(
                originalExpression,
                expression,
                argumentNames,
                argumentClasses,
                returnClass,
                columnNameMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformExpressionKey that = (TransformExpressionKey) o;
        return Objects.equals(originalExpression, that.originalExpression)
                && expression.equals(that.expression)
                && argumentNames.equals(that.argumentNames)
                && argumentClasses.equals(that.argumentClasses)
                && returnClass.equals(that.returnClass)
                && columnNameMap.equals(that.columnNameMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                originalExpression,
                expression,
                argumentNames,
                argumentClasses,
                returnClass,
                columnNameMap);
    }

    @Override
    public String toString() {
        return "TransformExpressionKey{"
                + "originalExpression='"
                + originalExpression
                + '\''
                + ", expression='"
                + expression
                + '\''
                + ", argumentNames="
                + argumentNames
                + ", argumentClasses="
                + argumentClasses
                + ", returnClass="
                + returnClass
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
