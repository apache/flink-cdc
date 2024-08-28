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

package org.apache.flink.cdc.runtime.functions;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import javax.annotation.Nullable;

import java.util.function.Function;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * This is the case when the operator has a special parsing syntax or uses other Calcite-specific
 * features that are not exposed via {@link SqlFunction} yet.
 */
@Internal
public class BuiltInScalarFunction extends SqlFunction {

    private final boolean isDeterministic;

    private final boolean isInternal;

    private final Function<SqlOperatorBinding, SqlMonotonicity> monotonicity;

    protected BuiltInScalarFunction(
            String name,
            SqlKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category,
            boolean isDeterministic,
            boolean isInternal,
            Function<SqlOperatorBinding, SqlMonotonicity> monotonicity) {
        super(
                checkNotNull(name),
                checkNotNull(kind),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                checkNotNull(category));
        this.isDeterministic = isDeterministic;
        this.isInternal = isInternal;
        this.monotonicity = monotonicity;
    }

    protected BuiltInScalarFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category) {
        this(
                name,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category,
                true,
                false,
                call -> SqlMonotonicity.NOT_MONOTONIC);
    }

    /** Builder for configuring and creating instances of {@link BuiltInScalarFunction}. */
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean isDeterministic() {
        return isDeterministic;
    }

    public final boolean isInternal() {
        return isInternal;
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return monotonicity.apply(call);
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    /** Builder for fluent definition of built-in functions. */
    public static class Builder {

        private String name;

        private SqlKind kind = SqlKind.OTHER_FUNCTION;

        private SqlReturnTypeInference returnTypeInference;

        private SqlOperandTypeInference operandTypeInference;

        private SqlOperandTypeChecker operandTypeChecker;

        private SqlFunctionCategory category = SqlFunctionCategory.SYSTEM;

        private boolean isInternal = false;

        private boolean isDeterministic = true;

        private Function<SqlOperatorBinding, SqlMonotonicity> monotonicity =
                call -> SqlMonotonicity.NOT_MONOTONIC;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder kind(SqlKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder returnType(SqlReturnTypeInference returnTypeInference) {
            this.returnTypeInference = returnTypeInference;
            return this;
        }

        public Builder operandTypeInference(SqlOperandTypeInference operandTypeInference) {
            this.operandTypeInference = operandTypeInference;
            return this;
        }

        public Builder operandTypeChecker(SqlOperandTypeChecker operandTypeChecker) {
            this.operandTypeChecker = operandTypeChecker;
            return this;
        }

        public Builder category(SqlFunctionCategory category) {
            this.category = category;
            return this;
        }

        public Builder notDeterministic() {
            this.isDeterministic = false;
            return this;
        }

        public Builder internal() {
            this.isInternal = true;
            return this;
        }

        public Builder monotonicity(SqlMonotonicity staticMonotonicity) {
            this.monotonicity = call -> staticMonotonicity;
            return this;
        }

        public Builder monotonicity(Function<SqlOperatorBinding, SqlMonotonicity> monotonicity) {
            this.monotonicity = monotonicity;
            return this;
        }

        public BuiltInScalarFunction build() {
            return new BuiltInScalarFunction(
                    name,
                    kind,
                    returnTypeInference,
                    operandTypeInference,
                    operandTypeChecker,
                    category,
                    isDeterministic,
                    isInternal,
                    monotonicity);
        }
    }
}
