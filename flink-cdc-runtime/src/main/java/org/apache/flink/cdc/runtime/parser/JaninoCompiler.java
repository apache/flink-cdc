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

package org.apache.flink.cdc.runtime.parser;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.cdc.common.utils.StringUtils;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Use Janino compiler to compiler the statement of flink cdc pipeline transform into the executable
 * code of Janino. For example, compiler 'string1 || string2' into 'concat(string1, string2)'. The
 * core logic is to traverse SqlNode tree and transform to Atom tree. Janino documents:
 * https://www.janino.net/index.html#properties
 */
public class JaninoCompiler {

    private static final List<SqlTypeName> SQL_TYPE_NAME_IGNORE = Arrays.asList(SqlTypeName.SYMBOL);
    private static final List<String> TIMEZONE_FREE_TEMPORAL_FUNCTIONS =
            Arrays.asList("CURRENT_TIMESTAMP", "NOW");

    private static final List<String> TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS =
            Arrays.asList("LOCALTIME", "LOCALTIMESTAMP", "CURRENT_TIME", "CURRENT_DATE");

    private static final List<String> TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS =
            Arrays.asList("DATE_FORMAT");

    private static final List<String> TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS =
            Arrays.asList("TO_DATE", "TO_TIMESTAMP");

    public static final String DEFAULT_EPOCH_TIME = "__epoch_time__";
    public static final String DEFAULT_TIME_ZONE = "__time_zone__";

    public static String loadSystemFunction(String expression) {
        return "import static org.apache.flink.cdc.runtime.functions.SystemFunctionUtils.*;"
                + expression;
    }

    public static ExpressionEvaluator compileExpression(
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass) {
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.setParameters(
                argumentNames.toArray(new String[0]), argumentClasses.toArray(new Class[0]));
        expressionEvaluator.setExpressionType(returnClass);
        try {
            expressionEvaluator.cook(expression);
            return expressionEvaluator;
        } catch (CompileException e) {
            throw new InvalidProgramException(
                    "Expression cannot be compiled. This is a bug. Please file an issue.\nExpression: "
                            + expression,
                    e);
        }
    }

    public static String translateSqlNodeToJaninoExpression(SqlNode transform) {
        Java.Rvalue rvalue = translateSqlNodeToJaninoRvalue(transform);
        if (rvalue != null) {
            return rvalue.toString();
        }
        return "";
    }

    public static Java.Rvalue translateSqlNodeToJaninoRvalue(SqlNode transform) {
        if (transform instanceof SqlIdentifier) {
            return translateSqlIdentifier((SqlIdentifier) transform);
        } else if (transform instanceof SqlBasicCall) {
            return translateSqlBasicCall((SqlBasicCall) transform);
        } else if (transform instanceof SqlCase) {
            return translateSqlCase((SqlCase) transform);
        } else if (transform instanceof SqlLiteral) {
            return translateSqlSqlLiteral((SqlLiteral) transform);
        }
        return null;
    }

    private static Java.Rvalue translateSqlIdentifier(SqlIdentifier sqlIdentifier) {
        String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(columnName)) {
            return generateTimezoneFreeTemporalFunctionOperation(columnName);
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(columnName)) {
            return generateTimezoneRequiredTemporalFunctionOperation(columnName);
        } else if (TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS.contains(columnName)) {
            return generateTimezoneFreeTemporalConversionFunctionOperation(columnName);
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(columnName)) {
            return generateTimezoneRequiredTemporalConversionFunctionOperation(columnName);
        } else {
            return new Java.AmbiguousName(Location.NOWHERE, new String[] {columnName});
        }
    }

    private static Java.Rvalue translateSqlSqlLiteral(SqlLiteral sqlLiteral) {
        if (sqlLiteral.getValue() == null) {
            return new Java.NullLiteral(Location.NOWHERE);
        }
        String value = sqlLiteral.getValue().toString();
        if (sqlLiteral instanceof SqlCharStringLiteral) {
            // Double quotation marks represent strings in Janino.
            value = "\"" + value.substring(1, value.length() - 1) + "\"";
        }
        if (SQL_TYPE_NAME_IGNORE.contains(sqlLiteral.getTypeName())) {
            value = "\"" + value + "\"";
        }
        return new Java.AmbiguousName(Location.NOWHERE, new String[] {value});
    }

    private static Java.Rvalue translateSqlBasicCall(SqlBasicCall sqlBasicCall) {
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        List<Java.Rvalue> atoms = new ArrayList<>();
        for (SqlNode sqlNode : operandList) {
            translateSqlNodeToAtoms(sqlNode, atoms);
        }
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(sqlBasicCall.getOperator().getName())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        }
        return sqlBasicCallToJaninoRvalue(sqlBasicCall, atoms.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue translateSqlCase(SqlCase sqlCase) {
        SqlNodeList whenOperands = sqlCase.getWhenOperands();
        SqlNodeList thenOperands = sqlCase.getThenOperands();
        SqlNode elseOperand = sqlCase.getElseOperand();
        List<Java.Rvalue> whenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : whenOperands) {
            translateSqlNodeToAtoms(sqlNode, whenAtoms);
        }
        List<Java.Rvalue> thenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : thenOperands) {
            translateSqlNodeToAtoms(sqlNode, thenAtoms);
        }
        Java.Rvalue elseAtoms = translateSqlNodeToJaninoRvalue(elseOperand);
        Java.Rvalue sqlCaseRvalueTemp = elseAtoms;
        for (int i = whenAtoms.size() - 1; i >= 0; i--) {
            sqlCaseRvalueTemp =
                    new Java.ConditionalExpression(
                            Location.NOWHERE,
                            whenAtoms.get(i),
                            thenAtoms.get(i),
                            sqlCaseRvalueTemp);
        }
        return new Java.ParenthesizedExpression(Location.NOWHERE, sqlCaseRvalueTemp);
    }

    private static void translateSqlNodeToAtoms(SqlNode sqlNode, List<Java.Rvalue> atoms) {
        if (sqlNode instanceof SqlIdentifier) {
            atoms.add(translateSqlIdentifier((SqlIdentifier) sqlNode));
        } else if (sqlNode instanceof SqlLiteral) {
            atoms.add(translateSqlSqlLiteral((SqlLiteral) sqlNode));
        } else if (sqlNode instanceof SqlBasicCall) {
            atoms.add(translateSqlBasicCall((SqlBasicCall) sqlNode));
        } else if (sqlNode instanceof SqlNodeList) {
            for (SqlNode node : (SqlNodeList) sqlNode) {
                translateSqlNodeToAtoms(node, atoms);
            }
        } else if (sqlNode instanceof SqlCase) {
            atoms.add(translateSqlCase((SqlCase) sqlNode));
        }
    }

    private static Java.Rvalue sqlBasicCallToJaninoRvalue(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        switch (sqlBasicCall.getKind()) {
            case AND:
                return generateBinaryOperation(sqlBasicCall, atoms, "&&");
            case OR:
                return generateBinaryOperation(sqlBasicCall, atoms, "||");
            case NOT:
                return generateUnaryOperation("!", atoms[0]);
            case EQUALS:
                return generateEqualsOperation(sqlBasicCall, atoms);
            case NOT_EQUALS:
                return generateUnaryOperation("!", generateEqualsOperation(sqlBasicCall, atoms));
            case IS_NULL:
                return generateUnaryOperation("null == ", atoms[0]);
            case IS_NOT_NULL:
                return generateUnaryOperation("null != ", atoms[0]);
            case IS_FALSE:
            case IS_NOT_TRUE:
                return generateUnaryOperation("false == ", atoms[0]);
            case IS_TRUE:
            case IS_NOT_FALSE:
                return generateUnaryOperation("true == ", atoms[0]);
            case BETWEEN:
            case IN:
            case NOT_IN:
            case LIKE:
            case CEIL:
            case FLOOR:
            case TRIM:
            case OTHER_FUNCTION:
                return generateOtherFunctionOperation(sqlBasicCall, atoms);
            case PLUS:
                return generateBinaryOperation(sqlBasicCall, atoms, "+");
            case MINUS:
                return generateBinaryOperation(sqlBasicCall, atoms, "-");
            case TIMES:
                return generateBinaryOperation(sqlBasicCall, atoms, "*");
            case DIVIDE:
                return generateBinaryOperation(sqlBasicCall, atoms, "/");
            case MOD:
                return generateBinaryOperation(sqlBasicCall, atoms, "%");
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                return generateBinaryOperation(sqlBasicCall, atoms, sqlBasicCall.getKind().sql);
            case CAST:
                return generateCastOperation(sqlBasicCall, atoms);
            case OTHER:
                return generateOtherOperation(sqlBasicCall, atoms);
            default:
                throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
    }

    private static Java.Rvalue generateUnaryOperation(String operator, Java.Rvalue atom) {
        return new Java.UnaryOperation(Location.NOWHERE, operator, atom);
    }

    private static Java.Rvalue generateBinaryOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms, String operator) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        return new Java.BinaryOperation(Location.NOWHERE, atoms[0], operator, atoms[1]);
    }

    private static Java.Rvalue generateEqualsOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase("VALUE_EQUALS"), atoms);
    }

    private static Java.Rvalue generateCastOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 1) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        SqlDataTypeSpec sqlDataTypeSpec = (SqlDataTypeSpec) operandList.get(1);
        return generateTypeConvertMethod(sqlDataTypeSpec, atoms);
    }

    private static Java.Rvalue generateOtherOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (sqlBasicCall.getOperator().getName().equals("||")) {
            return new Java.MethodInvocation(
                    Location.NOWHERE, null, StringUtils.convertToCamelCase("CONCAT"), atoms);
        }
        throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
    }

    private static Java.Rvalue generateOtherFunctionOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        String operationName = sqlBasicCall.getOperator().getName().toUpperCase();
        if (operationName.equals("IF")) {
            if (atoms.length == 3) {
                return new Java.ConditionalExpression(
                        Location.NOWHERE, atoms[0], atoms[1], atoms[2]);
            } else {
                throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
            }
        } else {
            return new Java.MethodInvocation(
                    Location.NOWHERE,
                    null,
                    StringUtils.convertToCamelCase(sqlBasicCall.getOperator().getName()),
                    atoms);
        }
    }

    private static Java.Rvalue generateTimezoneFreeTemporalFunctionOperation(String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME})
                });
    }

    private static Java.Rvalue generateTimezoneRequiredTemporalFunctionOperation(
            String operationName) {
        List<Java.Rvalue> timestampFunctionParam = new ArrayList<>();
        timestampFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
        timestampFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                timestampFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateTimezoneFreeTemporalConversionFunctionOperation(
            String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[0]);
    }

    private static Java.Rvalue generateTimezoneRequiredTemporalConversionFunctionOperation(
            String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE})
                });
    }

    private static Java.Rvalue generateTypeConvertMethod(
            SqlDataTypeSpec sqlDataTypeSpec, Java.Rvalue[] atoms) {
        switch (sqlDataTypeSpec.getTypeName().getSimple().toUpperCase()) {
            case "BOOLEAN":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToBoolean", atoms);
            case "TINYINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToByte", atoms);
            case "SMALLINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToShort", atoms);
            case "INTEGER":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToInteger", atoms);
            case "BIGINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToLong", atoms);
            case "FLOAT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToFloat", atoms);
            case "DOUBLE":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToDouble", atoms);
            case "DECIMAL":
                int precision = 10;
                int scale = 0;
                if (sqlDataTypeSpec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec) {
                    SqlBasicTypeNameSpec typeNameSpec =
                            (SqlBasicTypeNameSpec) sqlDataTypeSpec.getTypeNameSpec();
                    if (typeNameSpec.getPrecision() > -1) {
                        precision = typeNameSpec.getPrecision();
                    }
                    if (typeNameSpec.getScale() > -1) {
                        scale = typeNameSpec.getScale();
                    }
                }
                List<Java.Rvalue> newAtoms = new ArrayList<>(Arrays.asList(atoms));
                newAtoms.add(
                        new Java.AmbiguousName(
                                Location.NOWHERE, new String[] {String.valueOf(precision)}));
                newAtoms.add(
                        new Java.AmbiguousName(
                                Location.NOWHERE, new String[] {String.valueOf(scale)}));
                return new Java.MethodInvocation(
                        Location.NOWHERE,
                        null,
                        "castToBigDecimal",
                        newAtoms.toArray(new Java.Rvalue[0]));
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToString", atoms);
            case "TIMESTAMP":
                List<Java.Rvalue> timestampAtoms = new ArrayList<>(Arrays.asList(atoms));
                timestampAtoms.add(
                        new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
                return new Java.MethodInvocation(
                        Location.NOWHERE,
                        null,
                        "castToTimestamp",
                        timestampAtoms.toArray(new Java.Rvalue[0]));
            default:
                throw new ParseException(
                        "Unsupported data type cast: " + sqlDataTypeSpec.toString());
        }
    }
}
