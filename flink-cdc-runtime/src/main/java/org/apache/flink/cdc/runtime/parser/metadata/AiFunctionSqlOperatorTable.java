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

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.ai.AiEmbeddingFunctionDef;
import org.apache.flink.cdc.runtime.ai.AiTextFunctionDef;
import org.apache.flink.cdc.runtime.typeutils.CalciteDataTypeConverter;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;

import java.util.ArrayList;
import java.util.List;

/** Creates SqlOperatorTable from {@link AiTextFunctionDef} definitions. */
public class AiFunctionSqlOperatorTable {

    private AiFunctionSqlOperatorTable() {}

    /** Creates an SqlOperatorTable containing all AI functions defined in AiFunctionDef. */
    public static org.apache.calcite.sql.SqlOperatorTable create() {
        List<SqlFunction> functions = new ArrayList<>();
        for (AiTextFunctionDef def : AiTextFunctionDef.values()) {
            functions.add(createTextSqlFunction(def));
        }
        for (AiEmbeddingFunctionDef def : AiEmbeddingFunctionDef.values()) {
            functions.add(createEmbeddingSqlFunction(def));
        }
        return SqlOperatorTables.of(functions);
    }

    private static SqlFunction createTextSqlFunction(AiTextFunctionDef def) {
        return new SqlFunction(
                def.getFunctionName(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(SqlTypeName.VARIANT),
                null,
                OperandTypes.family(toSqlTypeFamiliesWithAdditionalParams(def.getInputType())),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    private static SqlFunction createEmbeddingSqlFunction(AiEmbeddingFunctionDef def) {
        return new SqlFunction(
                def.getFunctionName(),
                SqlKind.OTHER_FUNCTION,
                opBinding ->
                        CalciteDataTypeConverter.convertCalciteType(
                                opBinding.getTypeFactory(), def.getOutputType()),
                null,
                OperandTypes.family(SqlTypeFamily.STRING, toSqlTypeFamily(def.getInputType())),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Converts inputType to SqlTypeFamily array, prepending additional parameters: modelName
     * (STRING) and input (STRING).
     */
    private static SqlTypeFamily[] toSqlTypeFamiliesWithAdditionalParams(RowType inputType) {
        List<SqlTypeFamily> families = new ArrayList<>();
        families.add(SqlTypeFamily.STRING); // modelName
        families.add(SqlTypeFamily.STRING); // input
        for (DataType fieldType : inputType.getFieldTypes()) {
            families.add(toSqlTypeFamily(fieldType));
        }
        return families.toArray(new SqlTypeFamily[0]);
    }

    private static SqlTypeFamily toSqlTypeFamily(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return SqlTypeFamily.STRING;
            case INTEGER:
                return SqlTypeFamily.INTEGER;
            case BIGINT:
                return SqlTypeFamily.NUMERIC;
            case FLOAT:
            case DOUBLE:
                return SqlTypeFamily.APPROXIMATE_NUMERIC;
            case BOOLEAN:
                return SqlTypeFamily.BOOLEAN;
            default:
                return SqlTypeFamily.ANY;
        }
    }
}
