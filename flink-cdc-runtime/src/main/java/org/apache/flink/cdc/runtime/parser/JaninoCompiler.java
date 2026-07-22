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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.converter.JavaClassConverter;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
            Arrays.asList(
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    "CURRENT_TIME",
                    "CURRENT_DATE",
                    "UNIX_TIMESTAMP");

    private static final List<String> TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS =
            List.of("TO_DATE", "TO_TIMESTAMP_LTZ");

    private static final List<String> TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS =
            Arrays.asList(
                    "TO_TIMESTAMP",
                    "FROM_UNIXTIME",
                    "TIMESTAMPADD",
                    "TIMESTAMPDIFF",
                    "TIMESTAMP_DIFF",
                    "DATE_FORMAT",
                    "DATE_ADD");

    public static final String DEFAULT_EPOCH_TIME = "__epoch_time__";
    public static final String DEFAULT_TIME_ZONE = "__time_zone__";

    private static final String[] BUILTIN_FUNCTION_MODULES = {
        "Arithmetic", "Casting", "Comparison", "Logical", "String", "Struct", "Temporal"
    };

    @VisibleForTesting
    public static final String LOAD_MODULES_EXPRESSION =
            Arrays.stream(BUILTIN_FUNCTION_MODULES)
                    .map(
                            mod ->
                                    String.format(
                                            "import static org.apache.flink.cdc.runtime.functions.impl.%sFunctions.*;",
                                            mod))
                    .collect(Collectors.joining());

    public static String loadSystemFunction(String expression) {
        return LOAD_MODULES_EXPRESSION + expression;
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

    public static String translateSqlNodeToJaninoExpression(Context context, SqlNode transform) {
        Java.Rvalue rvalue = translateSqlNodeToJaninoRvalue(context, transform);
        if (rvalue != null) {
            return rvalue.toString();
        }
        return "";
    }

    public static GeneratedExpression translateSqlNodeToGeneratedExpression(
            Context context, SqlNode transform, Class<?> resultClass) {
        return new GeneratedExpressionGenerator(context).translate(transform, resultClass);
    }

    public static GeneratedExpression translateSqlNodeToGeneratedExpression(
            Context context, SqlNode transform) {
        return translateSqlNodeToGeneratedExpression(
                context, transform, deduceGeneratedExpressionClass(context, transform));
    }

    public static Java.Rvalue translateSqlNodeToJaninoRvalue(Context context, SqlNode transform) {
        if (transform instanceof SqlIdentifier) {
            return translateSqlIdentifier(context, (SqlIdentifier) transform);
        } else if (transform instanceof SqlBasicCall) {
            return translateSqlBasicCall(context, (SqlBasicCall) transform);
        } else if (transform instanceof SqlCase) {
            return translateSqlCase(context, (SqlCase) transform);
        } else if (transform instanceof SqlLiteral) {
            return translateSqlSqlLiteral(context, (SqlLiteral) transform);
        }
        return null;
    }

    private static Java.Rvalue translateSqlIdentifier(
            Context context, SqlIdentifier sqlIdentifier) {
        String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneFreeTemporalFunctionOperation(context, columnName);
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneRequiredTemporalFunctionOperation(context, columnName);
        } else if (TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneFreeTemporalConversionFunctionOperation(context, columnName);
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                columnName.toUpperCase())) {
            return generateTimezoneRequiredTemporalConversionFunctionOperation(context, columnName);
        } else {
            return new Java.AmbiguousName(
                    Location.NOWHERE,
                    new String[] {context.columnNameMap.getOrDefault(columnName, columnName)});
        }
    }

    private static Java.Rvalue translateSqlSqlLiteral(Context context, SqlLiteral sqlLiteral) {
        if (sqlLiteral.getValue() == null) {
            return new Java.NullLiteral(Location.NOWHERE);
        }
        Object value = sqlLiteral.getValue();
        if (sqlLiteral instanceof SqlCharStringLiteral) {
            // Double quotation marks represent strings in Janino.
            // Escape backslashes first, then double quotes for proper Janino string literals.
            String stringValue = sqlLiteral.getValueAs(NlsString.class).getValue();
            stringValue = stringValue.replace("\\", "\\\\").replace("\"", "\\\"");
            value = "\"" + stringValue + "\"";
        } else if (sqlLiteral instanceof SqlNumericLiteral) {
            if (((SqlNumericLiteral) sqlLiteral).isInteger()) {
                long longValue = sqlLiteral.longValue(true);
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    value += "L";
                }
            }
        }
        if (SQL_TYPE_NAME_IGNORE.contains(sqlLiteral.getTypeName())) {
            value = "\"" + value + "\"";
        }
        return new Java.AmbiguousName(Location.NOWHERE, new String[] {value.toString()});
    }

    private static Java.Rvalue translateSqlBasicCall(Context context, SqlBasicCall sqlBasicCall) {
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        List<Java.Rvalue> atoms = new ArrayList<>();
        for (SqlNode sqlNode : operandList) {
            translateSqlNodeToAtoms(context, sqlNode, atoms);
        }
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        }
        return sqlBasicCallToJaninoRvalue(context, sqlBasicCall, atoms.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue translateSqlCase(Context context, SqlCase sqlCase) {
        SqlNodeList whenOperands = sqlCase.getWhenOperands();
        SqlNodeList thenOperands = sqlCase.getThenOperands();
        SqlNode elseOperand = sqlCase.getElseOperand();
        List<Java.Rvalue> whenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : whenOperands) {
            translateSqlNodeToAtoms(context, sqlNode, whenAtoms);
        }
        List<Java.Rvalue> thenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : thenOperands) {
            translateSqlNodeToAtoms(context, sqlNode, thenAtoms);
        }
        Java.Rvalue elseAtoms = translateSqlNodeToJaninoRvalue(context, elseOperand);
        Java.Rvalue sqlCaseRvalueTemp = elseAtoms;
        for (int i = whenAtoms.size() - 1; i >= 0; i--) {
            sqlCaseRvalueTemp =
                    new Java.ConditionalExpression(
                            Location.NOWHERE,
                            generateFunctionOperation(
                                    "isTrue", new Java.Rvalue[] {whenAtoms.get(i)}),
                            thenAtoms.get(i),
                            sqlCaseRvalueTemp);
        }
        return new Java.ParenthesizedExpression(Location.NOWHERE, sqlCaseRvalueTemp);
    }

    private static void translateSqlNodeToAtoms(
            Context context, SqlNode sqlNode, List<Java.Rvalue> atoms) {
        if (sqlNode instanceof SqlIdentifier) {
            atoms.add(translateSqlIdentifier(context, (SqlIdentifier) sqlNode));
        } else if (sqlNode instanceof SqlLiteral) {
            atoms.add(translateSqlSqlLiteral(context, (SqlLiteral) sqlNode));
        } else if (sqlNode instanceof SqlBasicCall) {
            atoms.add(translateSqlBasicCall(context, (SqlBasicCall) sqlNode));
        } else if (sqlNode instanceof SqlNodeList) {
            for (SqlNode node : (SqlNodeList) sqlNode) {
                translateSqlNodeToAtoms(context, node, atoms);
            }
        } else if (sqlNode instanceof SqlCase) {
            atoms.add(translateSqlCase(context, (SqlCase) sqlNode));
        }
    }

    private static Java.Rvalue sqlBasicCallToJaninoRvalue(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        switch (sqlBasicCall.getKind()) {
            case AND:
                return generateLogicalBinaryOperation(context, sqlBasicCall, atoms, true);
            case OR:
                return generateLogicalBinaryOperation(context, sqlBasicCall, atoms, false);
            case NOT:
                return generateFunctionOperation("not", atoms);
            case EQUALS:
                return generateEqualsOperation(context, sqlBasicCall, atoms);
            case NOT_EQUALS:
                return generateUnaryOperation(
                        context, "!", generateEqualsOperation(context, sqlBasicCall, atoms));
            case IS_DISTINCT_FROM:
            case IS_NOT_DISTINCT_FROM:
                return generateOtherFunctionOperation(context, sqlBasicCall, atoms);
            case IS_NULL:
                return generateFunctionOperation("isNull", atoms);
            case IS_NOT_NULL:
                return generateFunctionOperation("isNotNull", atoms);
            case IS_FALSE:
                return generateFunctionOperation("isFalse", atoms);
            case IS_NOT_TRUE:
                return generateFunctionOperation("isNotTrue", atoms);
            case IS_TRUE:
                return generateFunctionOperation("isTrue", atoms);
            case IS_NOT_FALSE:
                return generateFunctionOperation("isNotFalse", atoms);
            case IS_UNKNOWN:
                if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("IS NOT UNKNOWN")) {
                    return generateFunctionOperation("isNotUnknown", atoms);
                }
                return generateFunctionOperation("isUnknown", atoms);
            case BETWEEN:
            case IN:
            case NOT_IN:
            case LIKE:
            case SIMILAR:
            case CEIL:
            case FLOOR:
            case TRIM:
            case COALESCE:
            case OTHER_FUNCTION:
                return generateOtherFunctionOperation(context, sqlBasicCall, atoms);
            case PLUS:
                return generateBinaryOperation(context, sqlBasicCall, atoms, "+");
            case MINUS:
                return generateBinaryOperation(context, sqlBasicCall, atoms, "-");
            case TIMES:
                return generateBinaryOperation(context, sqlBasicCall, atoms, "*");
            case DIVIDE:
                return generateBinaryOperation(context, sqlBasicCall, atoms, "/");
            case MOD:
                return generateBinaryOperation(context, sqlBasicCall, atoms, "%");
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                return generateCompareOperation(context, sqlBasicCall, atoms);
            case CAST:
                return generateCastOperation(context, sqlBasicCall, atoms);
            case TIMESTAMP_DIFF:
                return generateTimestampDiffOperation(context, sqlBasicCall, atoms);
            case TIMESTAMP_ADD:
                return generateTimestampAddOperation(context, sqlBasicCall, atoms);
            case OTHER:
                return generateOtherOperation(context, sqlBasicCall, atoms);
            case ITEM:
                return generateItemAccessOperation(context, sqlBasicCall, atoms);
            default:
                throw new ParseException("Unrecognized expression: " + sqlBasicCall);
        }
    }

    private static Java.Rvalue generateUnaryOperation(
            Context context, String operator, Java.Rvalue atom) {
        return new Java.UnaryOperation(Location.NOWHERE, operator, atom);
    }

    private static Java.Rvalue generateFunctionOperation(String functionName, Java.Rvalue[] atoms) {
        return new Java.MethodInvocation(Location.NOWHERE, null, functionName, atoms);
    }

    private static Java.Rvalue generateLogicalBinaryOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms, boolean isAnd) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        boolean leftNullable = isExpressionNullable(context, sqlBasicCall.getOperandList().get(0));
        boolean rightNullable = isExpressionNullable(context, sqlBasicCall.getOperandList().get(1));
        if (!leftNullable && !rightNullable) {
            return generateBinaryOperation(context, sqlBasicCall, atoms, isAnd ? "&&" : "||");
        }
        if (!leftNullable) {
            return generateLeftNonNullableLogicalOperation(atoms, isAnd);
        }
        return generateNullableLeftLogicalOperation(atoms, isAnd);
    }

    private static Java.Rvalue generateLeftNonNullableLogicalOperation(
            Java.Rvalue[] atoms, boolean isAnd) {
        if (isAnd) {
            return new Java.ConditionalExpression(
                    Location.NOWHERE,
                    atoms[0],
                    atoms[1],
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {"Boolean.FALSE"}));
        }
        return new Java.ConditionalExpression(
                Location.NOWHERE,
                atoms[0],
                new Java.AmbiguousName(Location.NOWHERE, new String[] {"Boolean.TRUE"}),
                atoms[1]);
    }

    private static Java.Rvalue generateNullableLeftLogicalOperation(
            Java.Rvalue[] atoms, boolean isAnd) {
        String conditionFunction = isAnd ? "isFalse" : "isTrue";
        String shortCircuitValue = isAnd ? "Boolean.FALSE" : "Boolean.TRUE";
        String logicalFunction = isAnd ? "and" : "or";
        return new Java.ConditionalExpression(
                Location.NOWHERE,
                generateFunctionOperation(conditionFunction, new Java.Rvalue[] {atoms[0]}),
                new Java.AmbiguousName(Location.NOWHERE, new String[] {shortCircuitValue}),
                generateFunctionOperation(logicalFunction, atoms));
    }

    private static boolean isExpressionNullable(Context context, SqlNode sqlNode) {
        boolean fallbackNullable = isExpressionNullableFallback(context, sqlNode);
        if (!fallbackNullable) {
            return false;
        }
        Optional<Boolean> calciteNullability =
                deduceExpressionNullableWithCalcite(context, sqlNode);
        if (calciteNullability.isPresent()) {
            return calciteNullability.get();
        }
        return true;
    }

    private static Optional<Boolean> deduceExpressionNullableWithCalcite(
            Context context, SqlNode sqlNode) {
        try {
            return Optional.of(
                    TransformParser.deduceSubExpressionNullable(
                            context.columns,
                            sqlNode,
                            context.udfDescriptors,
                            context.supportedMetadataColumns));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static boolean isExpressionNullableFallback(Context context, SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            return isIdentifierNullable(context, (SqlIdentifier) sqlNode);
        }
        if (sqlNode instanceof SqlLiteral) {
            return ((SqlLiteral) sqlNode).getValue() == null;
        }
        if (sqlNode instanceof SqlBasicCall) {
            return isBasicCallNullable(context, (SqlBasicCall) sqlNode);
        }
        return true;
    }

    private static boolean isIdentifierNullable(Context context, SqlIdentifier sqlIdentifier) {
        String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
        for (Column column : context.columns) {
            if (column.getName().equals(columnName)) {
                return column.getType().isNullable();
            }
        }
        for (SupportedMetadataColumn metadataColumn : context.supportedMetadataColumns) {
            if (metadataColumn.getName().equals(columnName)) {
                return metadataColumn.getType().isNullable();
            }
        }
        return MetadataColumns.METADATA_COLUMNS.stream()
                .filter(column -> column.f0.equals(columnName))
                .findFirst()
                .map(column -> column.f1.isNullable())
                .orElse(true);
    }

    private static boolean isBasicCallNullable(Context context, SqlBasicCall sqlBasicCall) {
        switch (sqlBasicCall.getKind()) {
            case AND:
            case OR:
                return sqlBasicCall.getOperandList().stream()
                        .anyMatch(operand -> isExpressionNullable(context, operand));
            case NOT:
                return isExpressionNullable(context, sqlBasicCall.getOperandList().get(0));
            case IS_NULL:
            case IS_NOT_NULL:
            case IS_FALSE:
            case IS_NOT_TRUE:
            case IS_TRUE:
            case IS_NOT_FALSE:
            case IS_UNKNOWN:
            case IS_DISTINCT_FROM:
            case IS_NOT_DISTINCT_FROM:
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
            case BETWEEN:
            case IN:
            case NOT_IN:
                return false;
            case LIKE:
            case SIMILAR:
                return sqlBasicCall.getOperandList().stream()
                        .anyMatch(operand -> isExpressionNullable(context, operand));
            default:
                return true;
        }
    }

    private static final Map<String, String> decimalArithmeticHandlers =
            Map.of(
                    "+", "plus",
                    "-", "minus",
                    "*", "times",
                    "/", "divides");

    private static Java.Rvalue generateBinaryOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms, String operator) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        if (decimalArithmeticHandlers.containsKey(operator)) {
            String handler = decimalArithmeticHandlers.get(operator);
            DataType resultType =
                    TransformParser.deduceSubExpressionType(
                            context.columns,
                            sqlBasicCall,
                            context.udfDescriptors,
                            context.supportedMetadataColumns);
            if (resultType.is(DataTypeRoot.DECIMAL)) {
                return new Java.MethodInvocation(Location.NOWHERE, null, handler, atoms);
            }
        }
        return new Java.BinaryOperation(Location.NOWHERE, atoms[0], operator, atoms[1]);
    }

    private static Java.Rvalue generateEqualsOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase("VALUE_EQUALS"), atoms);
    }

    private static Class<?> deduceGeneratedExpressionClass(Context context, SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            return deduceIdentifierClass(context, (SqlIdentifier) sqlNode);
        }
        if (sqlNode instanceof SqlLiteral) {
            return deduceLiteralClass((SqlLiteral) sqlNode);
        }
        if (sqlNode instanceof SqlBasicCall && isBooleanResultCall((SqlBasicCall) sqlNode)) {
            return Boolean.class;
        }
        try {
            return JavaClassConverter.toJavaClass(
                    TransformParser.deduceSubExpressionType(
                            context.columns,
                            sqlNode,
                            context.udfDescriptors,
                            context.supportedMetadataColumns));
        } catch (RuntimeException e) {
            return Object.class;
        }
    }

    private static Class<?> deduceIdentifierClass(Context context, SqlIdentifier sqlIdentifier) {
        String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
        for (Column column : context.columns) {
            if (column.getName().equals(columnName)) {
                return JavaClassConverter.toJavaClass(column.getType());
            }
        }
        for (SupportedMetadataColumn metadataColumn : context.supportedMetadataColumns) {
            if (metadataColumn.getName().equals(columnName)) {
                return metadataColumn.getJavaClass();
            }
        }
        Optional<Class<?>> metadataColumnClass =
                MetadataColumns.METADATA_COLUMNS.stream()
                        .filter(column -> column.f0.equals(columnName))
                        .findFirst()
                        .map(column -> (Class<?>) column.f2);
        return metadataColumnClass.orElse(Object.class);
    }

    private static Class<?> deduceLiteralClass(SqlLiteral sqlLiteral) {
        if (sqlLiteral.getValue() == null) {
            return Object.class;
        }
        if (sqlLiteral instanceof SqlCharStringLiteral) {
            return String.class;
        }
        if (sqlLiteral instanceof SqlNumericLiteral) {
            SqlNumericLiteral numericLiteral = (SqlNumericLiteral) sqlLiteral;
            if (numericLiteral.isInteger()) {
                long longValue = numericLiteral.longValue(true);
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    return Long.class;
                }
                return Integer.class;
            }
            return Double.class;
        }
        if (sqlLiteral.getTypeName() == SqlTypeName.BOOLEAN) {
            return Boolean.class;
        }
        return Object.class;
    }

    private static boolean isBooleanResultCall(SqlBasicCall sqlBasicCall) {
        switch (sqlBasicCall.getKind()) {
            case AND:
            case OR:
            case NOT:
            case EQUALS:
            case NOT_EQUALS:
            case IS_DISTINCT_FROM:
            case IS_NOT_DISTINCT_FROM:
            case IS_NULL:
            case IS_NOT_NULL:
            case IS_FALSE:
            case IS_NOT_TRUE:
            case IS_TRUE:
            case IS_NOT_FALSE:
            case IS_UNKNOWN:
            case BETWEEN:
            case IN:
            case NOT_IN:
            case LIKE:
            case SIMILAR:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private static Java.Rvalue generateCastOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 1) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        SqlDataTypeSpec sqlDataTypeSpec = (SqlDataTypeSpec) operandList.get(1);
        return generateTypeConvertMethod(context, sqlDataTypeSpec, atoms);
    }

    private static Java.Rvalue generateCompareOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String compareMethodName;
        switch (sqlBasicCall.getKind()) {
            case LESS_THAN:
                compareMethodName = "LESS_THAN";
                break;
            case GREATER_THAN:
                compareMethodName = "GREATER_THAN";
                break;
            case LESS_THAN_OR_EQUAL:
                compareMethodName = "LESS_THAN_OR_EQUAL";
                break;
            case GREATER_THAN_OR_EQUAL:
                compareMethodName = "GREATER_THAN_OR_EQUAL";
                break;
            default:
                throw new ParseException(
                        "Unsupported binary relation operator: "
                                + sqlBasicCall.getKind().toString());
        }
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase(compareMethodName), atoms);
    }

    private static Java.Rvalue generateTimestampDiffOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 4) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String timeIntervalUnit = atoms[0].toString().toUpperCase();
        switch (timeIntervalUnit) {
            case "\"SECOND\"":
            case "\"MINUTE\"":
            case "\"HOUR\"":
            case "\"DAY\"":
            case "\"MONTH\"":
            case "\"YEAR\"":
                break;
            default:
                throw new ParseException(
                        "Unsupported time interval unit in timestamp diff function: "
                                + timeIntervalUnit);
        }
        List<Java.Rvalue> timestampDiffFunctionParam = new ArrayList<>();
        timestampDiffFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {timeIntervalUnit}));
        timestampDiffFunctionParam.add(atoms[1]);
        timestampDiffFunctionParam.add(atoms[2]);
        timestampDiffFunctionParam.add(atoms[3]);
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(sqlBasicCall.getOperator().getName()),
                timestampDiffFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateTimestampAddOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 4) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String timeIntervalUnit = atoms[0].toString().toUpperCase();
        switch (timeIntervalUnit) {
            case "\"SECOND\"":
            case "\"MINUTE\"":
            case "\"HOUR\"":
            case "\"DAY\"":
            case "\"MONTH\"":
            case "\"YEAR\"":
                break;
            default:
                throw new ParseException(
                        "Unsupported time interval unit in timestamp add function: "
                                + timeIntervalUnit);
        }
        List<Java.Rvalue> timestampDiffFunctionParam = new ArrayList<>();
        timestampDiffFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {timeIntervalUnit}));
        timestampDiffFunctionParam.add(atoms[1]);
        timestampDiffFunctionParam.add(atoms[2]);
        timestampDiffFunctionParam.add(atoms[3]);
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(sqlBasicCall.getOperator().getName()),
                timestampDiffFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateCharLengthOperation(Context context, Java.Rvalue[] atoms) {
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase("CHAR_LENGTH"), atoms);
    }

    private static Java.Rvalue generateOtherOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (sqlBasicCall.getOperator().getName().equals("||")) {
            return new Java.MethodInvocation(
                    Location.NOWHERE, null, StringUtils.convertToCamelCase("CONCAT"), atoms);
        }
        throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
    }

    private static Java.Rvalue generateItemAccessOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        Preconditions.checkArgument(
                atoms.length == 2,
                "Expecting item accessing call %s to have 2 operands, got %s actually",
                sqlBasicCall,
                List.of(atoms));
        Java.Rvalue methodInvocation =
                new Java.MethodInvocation(Location.NOWHERE, null, "itemAccess", atoms);

        // Deduce the return type and add a cast to ensure proper type conversion
        DataType resultType =
                TransformParser.deduceSubExpressionType(
                        context.columns,
                        sqlBasicCall,
                        context.udfDescriptors,
                        context.supportedMetadataColumns);

        // Get the Java class for the result type and add a cast
        // Use getCanonicalName() to correctly handle array types (e.g., byte[] instead of "[B")
        Class<?> javaClass = JavaClassConverter.toJavaClass(resultType);
        if (javaClass != null && javaClass != Object.class) {
            String canonicalName = javaClass.getCanonicalName();
            if (canonicalName != null) {
                return new Java.Cast(
                        Location.NOWHERE,
                        new Java.ReferenceType(
                                Location.NOWHERE,
                                new Java.Annotation[0],
                                canonicalName.split("\\."),
                                null),
                        methodInvocation);
            }
        }
        return methodInvocation;
    }

    private static Java.Rvalue generateOtherFunctionOperation(
            Context context, SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        String operationName = sqlBasicCall.getOperator().getName().toUpperCase();
        if (operationName.equals("IF")) {
            if (atoms.length == 3) {
                return new Java.ConditionalExpression(
                        Location.NOWHERE,
                        generateFunctionOperation("isTrue", new Java.Rvalue[] {atoms[0]}),
                        atoms[1],
                        atoms[2]);
            } else {
                throw new ParseException("Unrecognized expression: " + sqlBasicCall);
            }
        } else {
            Optional<UserDefinedFunctionDescriptor> udfFunctionOptional =
                    context.udfDescriptors.stream()
                            .filter(e -> e.getName().equalsIgnoreCase(operationName))
                            .findFirst();
            return udfFunctionOptional
                    .map(
                            udfFunction ->
                                    new Java.MethodInvocation(
                                            Location.NOWHERE,
                                            null,
                                            generateInvokeExpression(udfFunction),
                                            atoms))
                    .orElseGet(
                            () ->
                                    new Java.MethodInvocation(
                                            Location.NOWHERE,
                                            null,
                                            StringUtils.convertToCamelCase(
                                                    sqlBasicCall.getOperator().getName()),
                                            atoms));
        }
    }

    private static Java.Rvalue generateTimezoneFreeTemporalFunctionOperation(
            Context context, String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME})
                });
    }

    private static Java.Rvalue generateTimezoneRequiredTemporalFunctionOperation(
            Context context, String operationName) {
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
            Context context, String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[0]);
    }

    private static Java.Rvalue generateTimezoneRequiredTemporalConversionFunctionOperation(
            Context context, String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE})
                });
    }

    private static Java.Rvalue generateTypeConvertMethod(
            Context context, SqlDataTypeSpec sqlDataTypeSpec, Java.Rvalue[] atoms) {
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

    private static String generateInvokeExpression(UserDefinedFunctionDescriptor udfFunction) {
        if (udfFunction.getReturnTypeHint() != null) {
            return String.format(
                    "(%s) __instanceOf%s.eval",
                    JavaClassConverter.toJavaClass(udfFunction.getReturnTypeHint())
                            .getCanonicalName(),
                    udfFunction.getClassName());
        } else {
            return String.format("__instanceOf%s.eval", udfFunction.getClassName());
        }
    }

    private static class GeneratedExpressionGenerator {
        private final Context context;
        private int termId;

        private GeneratedExpressionGenerator(Context context) {
            this.context = context;
        }

        private GeneratedExpression translate(SqlNode sqlNode, Class<?> resultClass) {
            if (sqlNode instanceof SqlBasicCall) {
                return translateSqlBasicCall((SqlBasicCall) sqlNode, resultClass);
            }
            if (sqlNode instanceof SqlCase) {
                return translateSqlCase((SqlCase) sqlNode, resultClass);
            }
            Java.Rvalue rvalue = translateSqlNodeToJaninoRvalue(context, sqlNode);
            return GeneratedExpression.fromExpression(
                    rvalue == null ? "" : rvalue.toString(), resultClass);
        }

        private GeneratedExpression translateSqlBasicCall(
                SqlBasicCall sqlBasicCall, Class<?> resultClass) {
            switch (sqlBasicCall.getKind()) {
                case AND:
                    return translateLogicalBinaryOperation(sqlBasicCall, true);
                case OR:
                    return translateLogicalBinaryOperation(sqlBasicCall, false);
                case OTHER_FUNCTION:
                    if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("IF")) {
                        return translateIf(sqlBasicCall, resultClass);
                    }
                    return translateGenericBasicCall(sqlBasicCall, resultClass);
                default:
                    return translateGenericBasicCall(sqlBasicCall, resultClass);
            }
        }

        private GeneratedExpression translateLogicalBinaryOperation(
                SqlBasicCall sqlBasicCall, boolean isAnd) {
            List<SqlNode> operands = sqlBasicCall.getOperandList();
            if (operands.size() != 2) {
                throw new ParseException("Unrecognized expression: " + sqlBasicCall);
            }

            GeneratedExpression left = translate(operands.get(0), Boolean.class);
            GeneratedExpression right = translate(operands.get(1), Boolean.class);

            String resultTerm = newTerm("result");
            String leftTerm = newTerm("left");
            String rightTerm = newTerm("right");
            String shortCircuitValue = isAnd ? "FALSE" : "TRUE";
            String functionName = isAnd ? "and" : "or";

            StringBuilder code = new StringBuilder();
            appendCode(code, left.getCode());
            code.append("Boolean ").append(resultTerm).append(";\n");
            code.append("Boolean ")
                    .append(leftTerm)
                    .append(" = ")
                    .append(left.getResultTerm())
                    .append(";\n");
            code.append("if (Boolean.")
                    .append(shortCircuitValue)
                    .append(".equals(")
                    .append(leftTerm)
                    .append(")) {\n");
            code.append(resultTerm).append(" = Boolean.").append(shortCircuitValue).append(";\n");
            code.append("} else {\n");
            appendCode(code, right.getCode());
            code.append("Boolean ")
                    .append(rightTerm)
                    .append(" = ")
                    .append(right.getResultTerm())
                    .append(";\n");
            code.append(resultTerm)
                    .append(" = ")
                    .append(functionName)
                    .append("(")
                    .append(leftTerm)
                    .append(", ")
                    .append(rightTerm)
                    .append(");\n");
            code.append("}\n");

            return GeneratedExpression.of(code.toString(), resultTerm, Boolean.class);
        }

        private GeneratedExpression translateIf(SqlBasicCall sqlBasicCall, Class<?> resultClass) {
            List<SqlNode> operands = sqlBasicCall.getOperandList();
            if (operands.size() != 3) {
                throw new ParseException("Unrecognized expression: " + sqlBasicCall);
            }

            GeneratedExpression condition = translate(operands.get(0), Boolean.class);
            GeneratedExpression thenExpression = translate(operands.get(1), resultClass);
            GeneratedExpression elseExpression = translate(operands.get(2), resultClass);

            String resultTerm = newTerm("result");
            String conditionTerm = newTerm("condition");
            StringBuilder code = new StringBuilder();
            appendCode(code, condition.getCode());
            code.append(className(resultClass)).append(" ").append(resultTerm).append(";\n");
            code.append("Boolean ")
                    .append(conditionTerm)
                    .append(" = ")
                    .append(condition.getResultTerm())
                    .append(";\n");
            code.append("if (isTrue(").append(conditionTerm).append(")) {\n");
            appendCode(code, thenExpression.getCode());
            code.append(resultTerm)
                    .append(" = ")
                    .append(thenExpression.getResultTerm())
                    .append(";\n");
            code.append("} else {\n");
            appendCode(code, elseExpression.getCode());
            code.append(resultTerm)
                    .append(" = ")
                    .append(elseExpression.getResultTerm())
                    .append(";\n");
            code.append("}\n");

            return GeneratedExpression.of(code.toString(), resultTerm, resultClass);
        }

        private GeneratedExpression translateSqlCase(SqlCase sqlCase, Class<?> resultClass) {
            String resultTerm = newTerm("result");
            StringBuilder code = new StringBuilder();
            code.append(className(resultClass)).append(" ").append(resultTerm).append(";\n");
            appendCaseBranch(
                    code,
                    resultTerm,
                    sqlCase.getWhenOperands(),
                    sqlCase.getThenOperands(),
                    sqlCase.getElseOperand(),
                    0,
                    resultClass);
            return GeneratedExpression.of(code.toString(), resultTerm, resultClass);
        }

        private void appendCaseBranch(
                StringBuilder code,
                String resultTerm,
                SqlNodeList whenOperands,
                SqlNodeList thenOperands,
                SqlNode elseOperand,
                int index,
                Class<?> resultClass) {
            if (index >= whenOperands.size()) {
                GeneratedExpression elseExpression =
                        elseOperand == null
                                ? GeneratedExpression.fromExpression("null", resultClass)
                                : translate(elseOperand, resultClass);
                appendCode(code, elseExpression.getCode());
                code.append(resultTerm)
                        .append(" = ")
                        .append(elseExpression.getResultTerm())
                        .append(";\n");
                return;
            }

            GeneratedExpression whenExpression = translate(whenOperands.get(index), Boolean.class);
            GeneratedExpression thenExpression = translate(thenOperands.get(index), resultClass);
            String conditionTerm = newTerm("condition");
            appendCode(code, whenExpression.getCode());
            code.append("Boolean ")
                    .append(conditionTerm)
                    .append(" = ")
                    .append(whenExpression.getResultTerm())
                    .append(";\n");
            code.append("if (isTrue(").append(conditionTerm).append(")) {\n");
            appendCode(code, thenExpression.getCode());
            code.append(resultTerm)
                    .append(" = ")
                    .append(thenExpression.getResultTerm())
                    .append(";\n");
            code.append("} else {\n");
            appendCaseBranch(
                    code,
                    resultTerm,
                    whenOperands,
                    thenOperands,
                    elseOperand,
                    index + 1,
                    resultClass);
            code.append("}\n");
        }

        private GeneratedExpression translateGenericBasicCall(
                SqlBasicCall sqlBasicCall, Class<?> resultClass) {
            List<GeneratedExpression> atoms = new ArrayList<>();
            for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
                translateSqlNodeToGeneratedAtoms(sqlNode, atoms);
            }
            if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(
                    sqlBasicCall.getOperator().getName().toUpperCase())) {
                atoms.add(GeneratedExpression.fromExpression(DEFAULT_EPOCH_TIME, Long.class));
            } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(
                    sqlBasicCall.getOperator().getName().toUpperCase())) {
                atoms.add(GeneratedExpression.fromExpression(DEFAULT_EPOCH_TIME, Long.class));
                atoms.add(GeneratedExpression.fromExpression(DEFAULT_TIME_ZONE, String.class));
            } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                    sqlBasicCall.getOperator().getName().toUpperCase())) {
                atoms.add(GeneratedExpression.fromExpression(DEFAULT_TIME_ZONE, String.class));
            }

            StringBuilder code = new StringBuilder();
            Java.Rvalue[] rvalues = new Java.Rvalue[atoms.size()];
            for (int i = 0; i < atoms.size(); i++) {
                GeneratedExpression atom = atoms.get(i);
                appendCode(code, atom.getCode());
                rvalues[i] = toRvalue(atom);
            }
            Java.Rvalue rvalue = sqlBasicCallToJaninoRvalue(context, sqlBasicCall, rvalues);
            return GeneratedExpression.of(code.toString(), rvalue.toString(), resultClass);
        }

        private void translateSqlNodeToGeneratedAtoms(
                SqlNode sqlNode, List<GeneratedExpression> atoms) {
            if (sqlNode instanceof SqlNodeList) {
                for (SqlNode node : (SqlNodeList) sqlNode) {
                    translateSqlNodeToGeneratedAtoms(node, atoms);
                }
            } else if (sqlNode instanceof SqlIdentifier
                    || sqlNode instanceof SqlLiteral
                    || sqlNode instanceof SqlBasicCall
                    || sqlNode instanceof SqlCase) {
                atoms.add(translate(sqlNode, deduceGeneratedExpressionClass(context, sqlNode)));
            }
        }

        private String newTerm(String prefix) {
            return prefix + "$" + termId++;
        }

        private static Java.Rvalue toRvalue(GeneratedExpression generatedExpression) {
            return new Java.AmbiguousName(
                    Location.NOWHERE, new String[] {generatedExpression.getResultTerm()});
        }

        private static void appendCode(StringBuilder builder, String code) {
            if (code.isEmpty()) {
                return;
            }
            builder.append(code);
            if (code.charAt(code.length() - 1) != '\n') {
                builder.append('\n');
            }
        }

        private static String className(Class<?> clazz) {
            if (clazz.getCanonicalName() != null) {
                return clazz.getCanonicalName();
            }
            return clazz.getName();
        }
    }

    /** Contextual information for {@link JaninoCompiler}. */
    public static class Context {

        // Upstream physical columns
        public final List<Column> columns;

        // Mangled column name map to $1, $2...
        public final Map<String, String> columnNameMap;

        // User defined function signatures
        public final List<UserDefinedFunctionDescriptor> udfDescriptors;

        // Readable metadata columns
        public final SupportedMetadataColumn[] supportedMetadataColumns;

        private Context(
                List<Column> columns,
                Map<String, String> columnNameMap,
                List<UserDefinedFunctionDescriptor> udfDescriptors,
                SupportedMetadataColumn[] supportedMetadataColumns) {
            this.columns = columns;
            this.columnNameMap = columnNameMap;
            this.udfDescriptors = udfDescriptors;
            this.supportedMetadataColumns = supportedMetadataColumns;
        }

        public static Context of(
                List<Column> columns,
                Map<String, String> columnNameMap,
                List<UserDefinedFunctionDescriptor> udfDescriptors,
                SupportedMetadataColumn[] supportedMetadataColumns) {
            return new Context(columns, columnNameMap, udfDescriptors, supportedMetadataColumns);
        }
    }
}
