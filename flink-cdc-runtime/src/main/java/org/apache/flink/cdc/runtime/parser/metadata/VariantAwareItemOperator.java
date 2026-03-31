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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * A variant-aware implementation of the ITEM operator.
 *
 * <p>This extends the standard Calcite ITEM operator to support VARIANT type. For ARRAY, MAP, and
 * ROW types, it delegates to the standard Calcite ITEM operator.
 *
 * <p>When accessing elements of a VARIANT:
 *
 * <ul>
 *   <li>VARIANT[INTEGER] - accesses array elements (1-based index)
 *   <li>VARIANT[STRING] - accesses object fields by name
 * </ul>
 *
 * <p>The return type is always VARIANT when the source is VARIANT.
 */
public class VariantAwareItemOperator extends SqlSpecialOperator {

    public VariantAwareItemOperator() {
        super("ITEM", SqlKind.ITEM, 100, true, null, null, null);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlStdOperatorTable.ITEM.getOperandCountRange();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        final SqlValidator validator = callBinding.getValidator();
        final RelDataType operandType =
                validator.deriveType(callBinding.getScope(), callBinding.operand(0));
        final SqlTypeName typeName = operandType.getSqlTypeName();

        // Only handle VARIANT specially, delegate others to standard ITEM
        if (typeName == SqlTypeName.VARIANT) {
            final RelDataType keyType =
                    validator.deriveType(callBinding.getScope(), callBinding.operand(1));
            // VARIANT accepts integer or string keys
            boolean valid =
                    SqlTypeUtil.isIntType(keyType)
                            || SqlTypeUtil.isCharacter(keyType)
                            || keyType.getSqlTypeName() == SqlTypeName.ANY;
            if (!valid && throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return valid;
        }

        // Delegate to standard ITEM for ARRAY, MAP, ROW, etc.
        return SqlStdOperatorTable.ITEM.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        final RelDataType operandType = opBinding.getOperandType(0);
        final SqlTypeName typeName = operandType.getSqlTypeName();

        // Only handle VARIANT specially
        if (typeName == SqlTypeName.VARIANT) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.VARIANT), true);
        }

        // Delegate to standard ITEM for other types
        return SqlStdOperatorTable.ITEM.inferReturnType(opBinding);
    }

    @Override
    public String getAllowedSignatures(String opName) {
        // Include VARIANT in addition to standard signatures
        return SqlStdOperatorTable.ITEM.getAllowedSignatures(opName)
                + "\n<VARIANT>[<INTEGER>|<CHARACTER>]";
    }

    @Override
    public void unparse(
            SqlWriter writer, org.apache.calcite.sql.SqlCall call, int leftPrec, int rightPrec) {
        SqlStdOperatorTable.ITEM.unparse(writer, call, leftPrec, rightPrec);
    }
}
