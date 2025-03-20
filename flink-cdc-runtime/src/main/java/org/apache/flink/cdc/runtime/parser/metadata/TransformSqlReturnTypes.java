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

package org.apache.flink.cdc.runtime.parser.metadata;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the return type definition of the call in the Transform expression, which references the
 * class of FlinkReturnTypes in the Flink. In addition, its difference is that it is specifically
 * referenced for FlinkCDC's Transform, and there may be new extensions here in the future.
 */
public class TransformSqlReturnTypes {

    /** ROUND(num [,len]) type inference. */
    public static final SqlReturnTypeInference ROUND_FUNCTION =
            new SqlReturnTypeInference() {
                @Override
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                    final RelDataType numType = opBinding.getOperandType(0);
                    if (numType.getSqlTypeName() != SqlTypeName.DECIMAL) {
                        return numType;
                    }
                    final BigDecimal lenVal;
                    if (opBinding.getOperandCount() == 1) {
                        lenVal = BigDecimal.ZERO;
                    } else if (opBinding.getOperandCount() == 2) {
                        lenVal = getArg1Literal(opBinding); // may return null
                    } else {
                        throw new AssertionError();
                    }
                    if (lenVal == null) {
                        return numType; //
                    }
                    // ROUND( decimal(p,s), r )
                    final int p = numType.getPrecision();
                    final int s = numType.getScale();
                    final int r = lenVal.intValueExact();
                    DecimalType dt = LogicalTypeMerging.findRoundDecimalType(p, s, r);
                    return opBinding
                            .getTypeFactory()
                            .createSqlType(SqlTypeName.DECIMAL, dt.getPrecision(), dt.getScale());
                }

                private BigDecimal getArg1Literal(SqlOperatorBinding opBinding) {
                    try {
                        return opBinding.getOperandLiteralValue(1, BigDecimal.class);
                    } catch (Throwable e) {
                        return null;
                    }
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #0
     * (0-based), with nulls always allowed.
     */
    public static final SqlReturnTypeInference ARG0_VARCHAR_FORCE_NULLABLE =
            new OrdinalReturnTypeInference(0) {
                @Override
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                    RelDataType type = super.inferReturnType(opBinding);
                    RelDataType newType;
                    switch (type.getSqlTypeName()) {
                        case CHAR:
                            newType =
                                    opBinding
                                            .getTypeFactory()
                                            .createSqlType(
                                                    SqlTypeName.VARCHAR, type.getPrecision());
                            break;
                        case VARCHAR:
                            newType = type;
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                    return opBinding.getTypeFactory().createTypeWithNullability(newType, true);
                }
            };

    public static final SqlReturnTypeInference VARCHAR_FORCE_NULLABLE =
            ReturnTypes.cascade(
                    ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.FORCE_NULLABLE);

    public static final SqlReturnTypeInference VARCHAR_NOT_NULL =
            ReturnTypes.cascade(
                    ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NOT_NULLABLE);

    public static final SqlReturnTypeInference ROUND_FUNCTION_NULLABLE =
            ReturnTypes.cascade(ROUND_FUNCTION, SqlTypeTransforms.TO_NULLABLE);

    /**
     * Determine the return type of IF functions with arguments that has the least restrictive (eg:
     * numeric, character, binary). The return type is the type of the argument with the largest
     * range. We start to consider the arguments from the first one. If one of the arguments is not
     * of the type that has the least restrictive (eg: numeric, character, binary), we return the
     * type of the first argument instead.
     */
    public static final SqlReturnTypeInference IF_NULLABLE =
            ReturnTypes.cascade(
                    new SqlReturnTypeInference() {
                        @Override
                        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                            int nOperands = opBinding.getOperandCount();
                            List<RelDataType> types = new ArrayList<>();
                            for (int i = 1; i < nOperands; i++) {
                                RelDataType type = opBinding.getOperandType(i);
                                // the RelDataTypeFactory.leastRestrictive() will check that all
                                // types are identical.
                                if (SqlTypeUtil.isNumeric(type)
                                        || SqlTypeUtil.isCharacter(type)
                                        || SqlTypeUtil.isBinary(type)) {
                                    types.add(type);
                                } else {
                                    return opBinding.getOperandType(1);
                                }
                            }
                            return opBinding.getTypeFactory().leastRestrictive(types);
                        }
                    },
                    SqlTypeTransforms.TO_NULLABLE);

    public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1 =
            new NumericOrDefaultReturnTypeInference(1, 1);

    public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1_NULLABLE =
            ReturnTypes.cascade(NUMERIC_FROM_ARG1_DEFAULT1, SqlTypeTransforms.TO_NULLABLE);

    private static class NumericOrDefaultReturnTypeInference implements SqlReturnTypeInference {
        // Default argument whose type is returned
        // when one of the arguments from the `startTypeIdx`-th isn't of numeric type.
        private int defaultTypeIdx;
        // We check from the `startTypeIdx`-th argument that
        // if all the following arguments are of numeric type.
        // Previous arguments are ignored.
        private int startTypeIdx;

        public NumericOrDefaultReturnTypeInference(int defaultTypeIdx) {
            this(defaultTypeIdx, 0);
        }

        public NumericOrDefaultReturnTypeInference(int defaultTypeIdx, int startTypeIdx) {
            this.defaultTypeIdx = defaultTypeIdx;
            this.startTypeIdx = startTypeIdx;
        }

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            int nOperands = opBinding.getOperandCount();
            List<RelDataType> types = new ArrayList<>();
            for (int i = startTypeIdx; i < nOperands; i++) {
                RelDataType type = opBinding.getOperandType(i);
                if (SqlTypeUtil.isNumeric(type)) {
                    types.add(type);
                } else {
                    return opBinding.getOperandType(defaultTypeIdx);
                }
            }
            return opBinding.getTypeFactory().leastRestrictive(types);
        }
    }

    public static final SqlReturnTypeInference ARG2_TIMESTAMP_FORCE_NULLABLE =
            new OrdinalReturnTypeInference(2) {};
}
