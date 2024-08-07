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

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSchemaFactory;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSqlOperatorTable;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.cdc.common.utils.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.flink.cdc.runtime.typeutils.DataTypeConverter.convertCalciteType;

/** Use Flink's calcite parser to parse the statement of flink cdc pipeline transform. */
public class TransformParser {
    private static final Logger LOG = LoggerFactory.getLogger(TransformParser.class);
    private static final String DEFAULT_SCHEMA = "default_schema";
    private static final String DEFAULT_TABLE = "TB";
    public static final String DEFAULT_NAMESPACE_NAME = "__namespace_name__";
    public static final String DEFAULT_SCHEMA_NAME = "__schema_name__";
    public static final String DEFAULT_TABLE_NAME = "__table_name__";

    private static SqlParser getCalciteParser(String sql) {
        return SqlParser.create(
                sql,
                SqlParser.Config.DEFAULT
                        .withConformance(SqlConformanceEnum.MYSQL_5)
                        .withCaseSensitive(true)
                        .withLex(Lex.JAVA));
    }

    private static RelNode sqlToRel(
            List<Column> columns,
            SqlNode sqlNode,
            List<UserDefinedFunctionDescriptor> udfDescriptors) {
        List<Column> columnsWithMetadata = copyFillMetadataColumn(columns);
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schema = rootSchema.plus();
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", DEFAULT_TABLE);
        operand.put("columns", columnsWithMetadata);
        rootSchema.add(
                DEFAULT_SCHEMA,
                TransformSchemaFactory.INSTANCE.create(schema, DEFAULT_SCHEMA, operand));
        List<SqlFunction> udfFunctions = new ArrayList<>();
        for (UserDefinedFunctionDescriptor udf : udfDescriptors) {
            try {
                Class<?> clazz = Class.forName(udf.getClasspath());
                SqlReturnTypeInference returnTypeInference;
                ScalarFunction function = ScalarFunctionImpl.create(clazz, "eval");
                if (udf.getReturnTypeHint() != null) {
                    // This UDF has return type hint annotation
                    returnTypeInference =
                            o ->
                                    o.getTypeFactory()
                                            .createSqlType(
                                                    convertCalciteType(udf.getReturnTypeHint()));
                } else {
                    // Infer it from eval method return type
                    returnTypeInference = o -> function.getReturnType(o.getTypeFactory());
                }
                schema.add(udf.getName(), function);
                udfFunctions.add(
                        new SqlFunction(
                                udf.getName(),
                                SqlKind.OTHER_FUNCTION,
                                returnTypeInference,
                                InferTypes.RETURN_TYPE,
                                OperandTypes.VARIADIC,
                                SqlFunctionCategory.USER_DEFINED_FUNCTION));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to resolve UDF: " + udf, e);
            }
        }
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path(DEFAULT_SCHEMA),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlOperatorTable udfOperatorTable = SqlOperatorTables.of(udfFunctions);
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(transformSqlOperatorTable, udfOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(sqlNode);
        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        null,
                        validator,
                        calciteCatalogReader,
                        RelOptCluster.create(
                                new HepPlanner(new HepProgramBuilder().build()),
                                new RexBuilder(factory)),
                        StandardConvertletTable.INSTANCE,
                        SqlToRelConverter.config().withTrimUnusedFields(false));
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, true);
        return relRoot.rel;
    }

    public static SqlSelect parseSelect(String statement) {
        SqlNode sqlNode = null;
        try {
            sqlNode = getCalciteParser(statement).parseQuery();
        } catch (SqlParseException e) {
            LOG.error("Statements can not be parsed. {} \n {}", statement, e);
            throw new ParseException("Statements can not be parsed.", e);
        }
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else {
            throw new ParseException("Only select statements can be parsed.");
        }
    }

    // Returns referenced columns (directly and indirectly) by projection and filter expression.
    // For example, given projection expression "a, c, upper(x) as d, y as e", filter expression "z
    // > 0", and columns array [a, b, c, x, y, z], returns referenced column array [a, c, x, y, z].
    public static List<Column> generateReferencedColumns(
            String projectionExpression, @Nullable String filterExpression, List<Column> columns) {
        if (isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }

        Set<String> referencedColumnNames = new HashSet<>();

        SqlSelect sqlProject = parseProjectionExpression(projectionExpression);
        if (!sqlProject.getSelectList().isEmpty()) {
            for (SqlNode sqlNode : sqlProject.getSelectList()) {
                if (sqlNode instanceof SqlBasicCall) {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                        referencedColumnNames.addAll(
                                parseColumnNameList(sqlBasicCall.getOperandList().get(0)));
                    } else {
                        throw new ParseException(
                                "Unrecognized projection expression: "
                                        + sqlBasicCall
                                        + ". Should be <EXPR> AS <IDENTIFIER>");
                    }
                } else if (sqlNode instanceof SqlIdentifier) {
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                    if (sqlIdentifier.isStar()) {
                        // wildcard star character matches all columns
                        return columns;
                    }
                    referencedColumnNames.add(
                            sqlIdentifier.names.get(sqlIdentifier.names.size() - 1));
                }
            }
        }

        if (!isNullOrWhitespaceOnly(projectionExpression)) {
            SqlSelect sqlFilter = parseFilterExpression(filterExpression);
            referencedColumnNames.addAll(parseColumnNameList(sqlFilter.getWhere()));
        }

        return columns.stream()
                .filter(e -> referencedColumnNames.contains(e.getName()))
                .collect(Collectors.toList());
    }

    // Expands wildcard character * to full column list.
    // For example, given projection expression "a AS new_a, *, c as new_c"
    // and schema [a, b, c], expand it to [a as new_a, a, b, c, c as new_c].
    // This step is necessary since passing wildcard to sqlToRel will capture
    // unexpected metadata columns.
    private static void expandWildcard(SqlSelect sqlSelect, List<Column> columns) {
        List<SqlNode> expandedNodes = new ArrayList<>();
        for (SqlNode sqlNode : sqlSelect.getSelectList().getList()) {
            if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).isStar()) {
                expandedNodes.addAll(
                        columns.stream()
                                .map(c -> new SqlIdentifier(c.getName(), SqlParserPos.QUOTED_ZERO))
                                .collect(Collectors.toList()));
            } else {
                expandedNodes.add(sqlNode);
            }
        }
        sqlSelect.setSelectList(new SqlNodeList(expandedNodes, SqlParserPos.ZERO));
    }

    // Returns projected columns based on given projection expression.
    // For example, given projection expression "a, b, c, upper(a) as d, b as e" and columns array
    // [a, b, c, x, y, z], returns projection column array [a, b, c, d, e].
    public static List<ProjectionColumn> generateProjectionColumns(
            String projectionExpression,
            List<Column> columns,
            List<UserDefinedFunctionDescriptor> udfDescriptors) {
        if (isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseProjectionExpression(projectionExpression);
        if (sqlSelect.getSelectList().isEmpty()) {
            return new ArrayList<>();
        }
        expandWildcard(sqlSelect, columns);
        RelNode relNode = sqlToRel(columns, sqlSelect, udfDescriptors);
        Map<String, RelDataType> relDataTypeMap =
                relNode.getRowType().getFieldList().stream()
                        .collect(
                                Collectors.toMap(
                                        RelDataTypeField::getName, RelDataTypeField::getType));

        Map<String, DataType> rawDataTypeMap =
                columns.stream().collect(Collectors.toMap(Column::getName, Column::getType));

        Map<String, Boolean> isNotNullMap =
                columns.stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName, column -> !column.getType().isNullable()));

        List<ProjectionColumn> projectionColumns = new ArrayList<>();

        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    Optional<SqlNode> transformOptional = Optional.empty();
                    String columnName;
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    if (operandList.size() == 2) {
                        transformOptional = Optional.of(operandList.get(0));
                        SqlNode sqlNode1 = operandList.get(1);
                        if (sqlNode1 instanceof SqlIdentifier) {
                            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode1;
                            columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                        } else {
                            columnName = null;
                        }
                    } else {
                        columnName = null;
                    }
                    if (isMetadataColumn(columnName)) {
                        continue;
                    }
                    ProjectionColumn projectionColumn =
                            transformOptional.isPresent()
                                    ? ProjectionColumn.of(
                                            columnName,
                                            DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                                    relDataTypeMap.get(columnName)),
                                            transformOptional.get().toString(),
                                            JaninoCompiler.translateSqlNodeToJaninoExpression(
                                                    transformOptional.get(), udfDescriptors),
                                            parseColumnNameList(transformOptional.get()))
                                    : ProjectionColumn.of(
                                            columnName,
                                            DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                                    relDataTypeMap.get(columnName)));
                    boolean hasReplacedDuplicateColumn = false;
                    for (int i = 0; i < projectionColumns.size(); i++) {
                        if (projectionColumns.get(i).getColumnName().equals(columnName)
                                && !projectionColumns.get(i).isValidTransformedProjectionColumn()) {
                            hasReplacedDuplicateColumn = true;
                            projectionColumns.set(i, projectionColumn);
                            break;
                        }
                    }
                    if (!hasReplacedDuplicateColumn) {
                        projectionColumns.add(projectionColumn);
                    }
                } else {
                    throw new ParseException(
                            "Unrecognized projection expression: "
                                    + sqlBasicCall
                                    + ". Should be <EXPR> AS <IDENTIFIER>");
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                DataType columnType;
                if (rawDataTypeMap.containsKey(columnName)) {
                    columnType = rawDataTypeMap.get(columnName);
                } else if (relDataTypeMap.containsKey(columnName)) {
                    columnType =
                            DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                    relDataTypeMap.get(columnName));
                } else {
                    throw new RuntimeException(
                            String.format("Failed to deduce column %s type", columnName));
                }
                if (isMetadataColumn(columnName)) {
                    projectionColumns.add(
                            ProjectionColumn.of(
                                    columnName,
                                    // Metadata columns should never be null
                                    columnType.notNull(),
                                    columnName,
                                    columnName,
                                    Arrays.asList(columnName)));
                } else {
                    // Calcite translated column type doesn't keep nullability.
                    // Appending it manually to circumvent this problem.
                    projectionColumns.add(
                            ProjectionColumn.of(
                                    columnName,
                                    isNotNullMap.get(columnName)
                                            ? columnType.notNull()
                                            : columnType.nullable()));
                }
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
        }
        return projectionColumns;
    }

    public static String translateFilterExpressionToJaninoExpression(
            String filterExpression, List<UserDefinedFunctionDescriptor> udfDescriptors) {
        if (isNullOrWhitespaceOnly(filterExpression)) {
            return "";
        }
        SqlSelect sqlSelect = TransformParser.parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return "";
        }
        SqlNode where = sqlSelect.getWhere();
        return JaninoCompiler.translateSqlNodeToJaninoExpression(where, udfDescriptors);
    }

    public static List<String> parseComputedColumnNames(String projection) {
        List<String> columnNames = new ArrayList<>();
        if (isNullOrWhitespaceOnly(projection)) {
            return columnNames;
        }
        SqlSelect sqlSelect = parseProjectionExpression(projection);
        if (sqlSelect.getSelectList().isEmpty()) {
            return columnNames;
        }
        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    String columnName = null;
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    for (SqlNode operand : operandList) {
                        if (operand instanceof SqlIdentifier) {
                            SqlIdentifier sqlIdentifier = (SqlIdentifier) operand;
                            columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                        }
                    }
                    if (columnNames.contains(columnName)) {
                        throw new ParseException("Duplicate column definitions: " + columnName);
                    }
                    columnNames.add(columnName);
                } else {
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall.toString());
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                String columnName = sqlNode.toString();
                if (isMetadataColumn(columnName) && !columnNames.contains(columnName)) {
                    columnNames.add(columnName);
                } else {
                    continue;
                }
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
        }
        return columnNames;
    }

    public static List<String> parseFilterColumnNameList(String filterExpression) {
        if (isNullOrWhitespaceOnly(filterExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return new ArrayList<>();
        }
        SqlNode where = sqlSelect.getWhere();
        return parseColumnNameList(where);
    }

    private static List<String> parseColumnNameList(SqlNode sqlNode) {
        List<String> columnNameList = new ArrayList<>();
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
            columnNameList.add(columnName);
        } else if (sqlNode instanceof SqlCall) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            findSqlIdentifier(sqlCall.getOperandList(), columnNameList);
        } else if (sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            findSqlIdentifier(sqlNodeList.getList(), columnNameList);
        }
        return columnNameList;
    }

    private static void findSqlIdentifier(List<SqlNode> sqlNodes, List<String> columnNameList) {
        for (SqlNode sqlNode : sqlNodes) {
            if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                columnNameList.add(columnName);
            } else if (sqlNode instanceof SqlCall) {
                SqlCall sqlCall = (SqlCall) sqlNode;
                findSqlIdentifier(sqlCall.getOperandList(), columnNameList);
            } else if (sqlNode instanceof SqlNodeList) {
                SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
                findSqlIdentifier(sqlNodeList.getList(), columnNameList);
            }
        }
    }

    private static SqlSelect parseProjectionExpression(String projection) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT ");
        statement.append(projection);
        statement.append(" FROM ");
        statement.append(DEFAULT_TABLE);
        return parseSelect(statement.toString());
    }

    private static List<Column> copyFillMetadataColumn(List<Column> columns) {
        // Add metaColumn for SQLValidator.validate
        List<Column> columnsWithMetadata = new ArrayList<>(columns);
        columnsWithMetadata.add(Column.physicalColumn(DEFAULT_NAMESPACE_NAME, DataTypes.STRING()));
        columnsWithMetadata.add(Column.physicalColumn(DEFAULT_SCHEMA_NAME, DataTypes.STRING()));
        columnsWithMetadata.add(Column.physicalColumn(DEFAULT_TABLE_NAME, DataTypes.STRING()));
        return columnsWithMetadata;
    }

    private static boolean isMetadataColumn(String columnName) {
        return DEFAULT_TABLE_NAME.equals(columnName)
                || DEFAULT_SCHEMA_NAME.equals(columnName)
                || DEFAULT_NAMESPACE_NAME.equals(columnName);
    }

    public static SqlSelect parseFilterExpression(String filterExpression) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT * FROM ");
        statement.append(DEFAULT_TABLE);
        if (!isNullOrWhitespaceOnly(filterExpression)) {
            statement.append(" WHERE ");
            statement.append(filterExpression);
        }
        return parseSelect(statement.toString());
    }

    public static SqlNode rewriteExpression(SqlNode sqlNode, Map<String, SqlNode> replaceMap) {
        if (sqlNode instanceof SqlCall) {
            SqlCall sqlCall = (SqlCall) sqlNode;

            List<SqlNode> operands = sqlCall.getOperandList();
            IntStream.range(0, sqlCall.operandCount())
                    .forEach(
                            i ->
                                    sqlCall.setOperand(
                                            i, rewriteExpression(operands.get(i), replaceMap)));
            return sqlCall;
        } else if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            if (sqlIdentifier.names.size() == 1) {
                String name = sqlIdentifier.names.get(0);
                if (replaceMap.containsKey(name)) {
                    return replaceMap.get(name);
                }
            }
            return sqlIdentifier;
        } else if (sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            IntStream.range(0, sqlNodeList.size())
                    .forEach(
                            i ->
                                    sqlNodeList.set(
                                            i, rewriteExpression(sqlNodeList.get(i), replaceMap)));
            return sqlNodeList;
        } else {
            return sqlNode;
        }
    }

    // Filter expression might hold reference to a calculated column, which causes confusion about
    // the sequence of projection and filtering operations. This function rewrites filtering about
    // calculated columns to circumvent this problem.
    public static String normalizeFilter(String projection, String filter) {
        if (isNullOrWhitespaceOnly(projection) || isNullOrWhitespaceOnly(filter)) {
            return filter;
        }

        SqlSelect sqlSelect = parseProjectionExpression(projection);
        if (sqlSelect.getSelectList().isEmpty()) {
            return filter;
        }

        Map<String, SqlNode> calculatedExpression = new HashMap<>();
        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    if (operandList.size() == 2) {
                        SqlIdentifier alias = (SqlIdentifier) operandList.get(1);
                        String name = alias.names.get(alias.names.size() - 1);
                        SqlNode expression = operandList.get(0);
                        calculatedExpression.put(name, expression);
                    }
                }
            }
        }

        SqlNode sqlFilter = parseFilterExpression(filter).getWhere();
        sqlFilter = rewriteExpression(sqlFilter, calculatedExpression);
        if (sqlFilter != null) {
            return sqlFilter.toString();
        } else {
            return filter;
        }
    }
}
