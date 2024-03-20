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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The key applies to describe the information of the transformation expression.
 *
 * <p>A transform expression key contains:
 *
 * <ul>
 *   <li>expression: a string for the transformation expression.
 *   <li>argumentNames: a list for the argument names in expression.
 *   <li>argumentClasses: a list for the argument classes in expression.
 *   <li>returnClass: a class for the return class in expression
 * </ul>
 */
public class TransformExpressionKey implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String expression;
    private final List<String> argumentNames;
    private final List<Class<?>> argumentClasses;
    private final Class<?> returnClass;

    private TransformExpressionKey(
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass) {
        this.expression = expression;
        this.argumentNames = argumentNames;
        this.argumentClasses = argumentClasses;
        this.returnClass = returnClass;
    }

    public String getExpression() {
        return expression;
    }

    public List<String> getArgumentNames() {
        return argumentNames;
    }

    public List<Class<?>> getArgumentClasses() {
        return argumentClasses;
    }

    public Class<?> getReturnClass() {
        return returnClass;
    }

    public static TransformExpressionKey of(
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass) {
        return new TransformExpressionKey(expression, argumentNames, argumentClasses, returnClass);
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
        return expression.equals(that.expression)
                && argumentNames.equals(that.argumentNames)
                && argumentClasses.equals(that.argumentClasses)
                && returnClass.equals(that.returnClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, argumentNames, argumentClasses, returnClass);
    }
}
