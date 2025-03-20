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

import org.apache.flink.cdc.runtime.functions.BuiltInScalarFunction;
import org.apache.flink.cdc.runtime.functions.BuiltInTimestampFunction;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import javax.annotation.Nullable;

import java.util.List;

/** TransformSqlOperatorTable to generate the metadata of calcite. */
public class TransformSqlOperatorTable extends ReflectiveSqlOperatorTable {

    private static TransformSqlOperatorTable instance;

    private TransformSqlOperatorTable() {}

    public static synchronized TransformSqlOperatorTable instance() {
        if (instance == null) {
            instance = new TransformSqlOperatorTable();
            instance.init();
        }

        return instance;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            @Nullable SqlFunctionCategory sqlFunctionCategory,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // set caseSensitive=false to make sure the behavior is same with before.
        super.lookupOperatorOverloads(
                opName,
                sqlFunctionCategory,
                syntax,
                operatorList,
                SqlNameMatchers.withCaseSensitive(false));
    }

    // The following binary functions are sorted in documentation definitions. See
    // https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/core-concept/transform/ for a
    // full list of CDC supported built-in functions.

    // --------------------
    // Comparison Functions
    // --------------------
    public static final SqlBinaryOperator EQUALS = SqlStdOperatorTable.EQUALS;
    public static final SqlBinaryOperator NOT_EQUALS = SqlStdOperatorTable.NOT_EQUALS;
    public static final SqlBinaryOperator GREATER_THAN = SqlStdOperatorTable.GREATER_THAN;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator LESS_THAN = SqlStdOperatorTable.LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL;

    public static final SqlPostfixOperator IS_NULL = SqlStdOperatorTable.IS_NULL;
    public static final SqlPostfixOperator IS_NOT_NULL = SqlStdOperatorTable.IS_NOT_NULL;

    public static final SqlBetweenOperator BETWEEN = SqlStdOperatorTable.BETWEEN;
    public static final SqlBetweenOperator NOT_BETWEEN = SqlStdOperatorTable.NOT_BETWEEN;

    public static final SqlSpecialOperator LIKE = SqlStdOperatorTable.LIKE;
    public static final SqlSpecialOperator NOT_LIKE = SqlStdOperatorTable.NOT_LIKE;

    public static final SqlBinaryOperator IN = SqlStdOperatorTable.IN;
    public static final SqlBinaryOperator NOT_IN = SqlStdOperatorTable.NOT_IN;

    // -----------------
    // Logical Functions
    // -----------------
    public static final SqlBinaryOperator OR = SqlStdOperatorTable.OR;
    public static final SqlBinaryOperator AND = SqlStdOperatorTable.AND;
    public static final SqlPrefixOperator NOT = SqlStdOperatorTable.NOT;

    public static final SqlPostfixOperator IS_FALSE = SqlStdOperatorTable.IS_FALSE;
    public static final SqlPostfixOperator IS_NOT_FALSE = SqlStdOperatorTable.IS_NOT_FALSE;
    public static final SqlPostfixOperator IS_TRUE = SqlStdOperatorTable.IS_TRUE;
    public static final SqlPostfixOperator IS_NOT_TRUE = SqlStdOperatorTable.IS_NOT_TRUE;

    // --------------------
    // Arithmetic Functions
    // --------------------
    public static final SqlBinaryOperator PLUS = SqlStdOperatorTable.PLUS;
    public static final SqlBinaryOperator MINUS = SqlStdOperatorTable.MINUS;
    public static final SqlBinaryOperator MULTIPLY = SqlStdOperatorTable.MULTIPLY;
    public static final SqlBinaryOperator DIVIDE = SqlStdOperatorTable.DIVIDE;
    public static final SqlBinaryOperator PERCENT_REMAINDER = SqlStdOperatorTable.PERCENT_REMAINDER;

    public static final SqlFunction ABS = SqlStdOperatorTable.ABS;
    public static final SqlFunction CEIL = SqlStdOperatorTable.CEIL;
    public static final SqlFunction FLOOR = SqlStdOperatorTable.FLOOR;
    public static final SqlFunction ROUND =
            new SqlFunction(
                    "ROUND",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.ROUND_FUNCTION_NULLABLE,
                    null,
                    OperandTypes.or(OperandTypes.NUMERIC_INTEGER, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);
    public static final SqlFunction UUID =
            BuiltInScalarFunction.newBuilder()
                    .name("UUID")
                    .returnType(ReturnTypes.explicit(SqlTypeName.CHAR, 36))
                    .operandTypeChecker(OperandTypes.NILADIC)
                    .notDeterministic()
                    .build();

    // ----------------
    // String Functions
    // ----------------
    public static final SqlBinaryOperator CONCAT = SqlStdOperatorTable.CONCAT;
    public static final SqlFunction CONCAT_FUNCTION =
            BuiltInScalarFunction.newBuilder()
                    .name("CONCAT")
                    .returnType(
                            ReturnTypes.cascade(
                                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                                    SqlTypeTransforms.TO_NULLABLE))
                    .operandTypeChecker(
                            OperandTypes.repeat(SqlOperandCountRanges.from(1), OperandTypes.STRING))
                    .build();

    public static final SqlFunction CHAR_LENGTH = SqlStdOperatorTable.CHAR_LENGTH;
    public static final SqlFunction UPPER = SqlStdOperatorTable.UPPER;
    public static final SqlFunction LOWER = SqlStdOperatorTable.LOWER;
    public static final SqlFunction TRIM = SqlStdOperatorTable.TRIM;
    public static final SqlFunction REGEXP_REPLACE =
            new SqlFunction(
                    "REGEXP_REPLACE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.VARCHAR),
                            SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
                    SqlFunctionCategory.STRING);
    public static final SqlFunction SUBSTR =
            new SqlFunction(
                    "SUBSTR",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.STRING);
    public static final SqlFunction SUBSTRING = SqlStdOperatorTable.SUBSTRING;

    // ------------------
    // Temporal Functions
    // ------------------
    public static final SqlFunction LOCALTIME = SqlStdOperatorTable.LOCALTIME;
    public static final SqlFunction LOCALTIMESTAMP =
            new BuiltInTimestampFunction("LOCALTIMESTAMP", SqlTypeName.TIMESTAMP, 3);
    public static final SqlFunction CURRENT_TIME =
            new BuiltInTimestampFunction("CURRENT_TIME", SqlTypeName.TIME, 0);
    public static final SqlFunction CURRENT_DATE = SqlStdOperatorTable.CURRENT_DATE;
    public static final SqlFunction CURRENT_TIMESTAMP =
            new BuiltInTimestampFunction(
                    "CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
    public static final SqlFunction NOW =
            new BuiltInTimestampFunction("NOW", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3) {
                @Override
                public SqlSyntax getSyntax() {
                    return SqlSyntax.FUNCTION;
                }
            };
    public static final SqlFunction UNIX_TIMESTAMP =
            new SqlFunction(
                    "UNIX_TIMESTAMP",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BIGINT_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.NILADIC,
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction FROM_UNIXTIME =
            new SqlFunction(
                    "FROM_UNIXTIME",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction DATE_FORMAT =
            new SqlFunction(
                    "DATE_FORMAT",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.VARCHAR_FORCE_NULLABLE,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING),
                            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction TIMESTAMP_DIFF =
            new SqlFunction(
                    "TIMESTAMP_DIFF",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.INTEGER),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction TIMESTAMPDIFF =
            new SqlFunction(
                    "TIMESTAMPDIFF",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.INTEGER),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction TIMESTAMPADD =
            new SqlFunction(
                    "TIMESTAMPADD",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.ARG2_TIMESTAMP_FORCE_NULLABLE,
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.TIMESTAMP),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction TO_DATE =
            new SqlFunction(
                    "TO_DATE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.DATE),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.STRING),
                            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
                    SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction TO_TIMESTAMP =
            new SqlFunction(
                    "TO_TIMESTAMP",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.cascade(
                            ReturnTypes.explicit(SqlTypeName.TIMESTAMP, 3),
                            SqlTypeTransforms.FORCE_NULLABLE),
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.TIMEDATE);

    // ---------------------
    // Conditional Functions
    // ---------------------
    public static final SqlCaseOperator CASE = SqlStdOperatorTable.CASE;
    public static final SqlFunction COALESCE = SqlStdOperatorTable.COALESCE;
    public static final SqlFunction IF =
            new SqlFunction(
                    "IF",
                    SqlKind.OTHER_FUNCTION,
                    TransformSqlReturnTypes.NUMERIC_FROM_ARG1_DEFAULT1_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.and(
                                    // cannot only use `family(BOOLEAN, NUMERIC, NUMERIC)` here,
                                    // as we don't want non-numeric types to be implicitly casted to
                                    // numeric types.
                                    new TransformNumericExceptFirstOperandChecker(3),
                                    OperandTypes.family(
                                            SqlTypeFamily.BOOLEAN,
                                            SqlTypeFamily.NUMERIC,
                                            SqlTypeFamily.NUMERIC)),
                            // used for a more explicit exception message
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.STRING,
                                    SqlTypeFamily.STRING),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BOOLEAN),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.CHARACTER),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.BINARY,
                                    SqlTypeFamily.BINARY),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN, SqlTypeFamily.DATE, SqlTypeFamily.DATE),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN,
                                    SqlTypeFamily.TIMESTAMP,
                                    SqlTypeFamily.TIMESTAMP),
                            OperandTypes.family(
                                    SqlTypeFamily.BOOLEAN, SqlTypeFamily.TIME, SqlTypeFamily.TIME)),
                    SqlFunctionCategory.NUMERIC);

    // --------------
    // Cast Functions
    // --------------
    public static final SqlFunction CAST = SqlStdOperatorTable.CAST;

    public static final SqlFunction AI_CHAT_PREDICT =
            new SqlFunction(
                    "AI_CHAT_PREDICT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);

    // Define the AI_EMBEDDING function
    public static final SqlFunction GET_EMBEDDING =
            new SqlFunction(
                    "GET_EMBEDDING",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);

    // Define the AI_LANGCHAIN_PREDICT function
    public static final SqlFunction AI_LANGCHAIN_PREDICT =
            new SqlFunction(
                    "AI_LANGCHAIN_PREDICT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                    null,
                    OperandTypes.family(
                            SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
}
