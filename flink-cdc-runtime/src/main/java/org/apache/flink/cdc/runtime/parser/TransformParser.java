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
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.utils.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;
import static org.apache.flink.cdc.runtime.typeutils.DataTypeConverter.convertCalciteType;

/** Use Flink's calcite parser to parse the statement of flink cdc pipeline transform. */
public class TransformParser {
    private static final Logger LOG = LoggerFactory.getLogger(TransformParser.class);
    private static final String DEFAULT_SCHEMA = "default_schema";
    private static final String DEFAULT_TABLE = "TB";
    private static final String MAPPED_COLUMN_NAME_PREFIX = "$";
    private static final String MAPPED_SINGLE_COLUMN_NAME = MAPPED_COLUMN_NAME_PREFIX + "0";

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
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        List<Column> columnsWithMetadata =
                copyFillMetadataColumn(columns, supportedMetadataColumns);
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
                Preconditions.checkNotNull(
                        function, "UDF function must provide at least one `eval` method.");
                if (udf.getReturnTypeHint() != null) {
                    // This UDF has return type hint annotation
                    returnTypeInference =
                            o -> {
                                RelDataTypeFactory typeFactory = o.getTypeFactory();
                                DataType returnTypeHint = udf.getReturnTypeHint();
                                return convertCalciteType(typeFactory, returnTypeHint);
                            };
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
                        SqlValidator.Config.DEFAULT
                                .withIdentifierExpansion(true)
                                .withConformance(SqlConformanceEnum.MYSQL_5));
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
                        SqlToRelConverter.config().withTrimUnusedFields(true));
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, false);
        return relRoot.rel;
    }

    public static SqlSelect parseSelect(String statement) {
        SqlNode sqlNode;
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
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        if (isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseProjectionExpression(projectionExpression);
        if (sqlSelect.getSelectList().isEmpty()) {
            return new ArrayList<>();
        }

        expandWildcard(sqlSelect, columns);
        RelNode relNode = sqlToRel(columns, sqlSelect, udfDescriptors, supportedMetadataColumns);
        RelDataType[] relDataTypes =
                relNode.getRowType().getFieldList().stream()
                        .map(RelDataTypeField::getType)
                        .toArray(RelDataType[]::new);
        Map<String, Column> originalColumnMap =
                columns.stream().collect(Collectors.toMap(Column::getName, column -> column));
        List<ProjectionColumn> projectionColumns = new ArrayList<>();
        Map<String, Integer> addedProjectionColumnNames = new HashMap<>();

        SqlNodeList selectExpressionList = sqlSelect.getSelectList();
        for (int i = 0; i < selectExpressionList.size(); i++) {
            SqlNode sqlNode = selectExpressionList.get(i);
            RelDataType relDataType = relDataTypes[i];
            ProjectionColumn projectionColumn;

            // A projection column could be <EXPR> AS <IDENTIFIER>...
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                List<SqlNode> operandList = sqlBasicCall.getOperandList();
                Preconditions.checkArgument(
                        SqlKind.AS.equals(sqlBasicCall.getOperator().kind)
                                && operandList.size() == 2
                                && operandList.get(1) instanceof SqlIdentifier,
                        "Unrecognized projection expression: "
                                + sqlBasicCall
                                + ". Should be <EXPR> AS <IDENTIFIER>");

                // It's the identifier node for aliased column.
                SqlIdentifier aliasNode = (SqlIdentifier) operandList.get(1);
                String columnName = aliasNode.names.get(aliasNode.names.size() - 1);

                Preconditions.checkArgument(
                        !isMetadataColumn(columnName, supportedMetadataColumns),
                        "Column name %s is reserved and shading it is not allowed.",
                        columnName);

                // This is the actual expression node of this projection column.
                SqlNode exprNode = operandList.get(0);

                if (exprNode instanceof SqlIdentifier) {
                    // This is a simple column rename like col_a AS col_b. Simply forward it to
                    // avoid losing metadata info like comments and default expressions.
                    SqlIdentifier identifierExprNode = (SqlIdentifier) exprNode;
                    String originalName =
                            identifierExprNode.names.get(identifierExprNode.names.size() - 1);
                    projectionColumn =
                            resolveProjectionColumnFromIdentifier(
                                    relDataType,
                                    originalColumnMap,
                                    originalName,
                                    columnName,
                                    supportedMetadataColumns);
                } else {
                    List<String> originalColumnNames = parseColumnNameList(exprNode);
                    Map<String, String> columnNameMap = generateColumnNameMap(originalColumnNames);
                    projectionColumn =
                            ProjectionColumn.ofCalculated(
                                    columnName,
                                    DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                            relDataType),
                                    exprNode.toString(),
                                    JaninoCompiler.translateSqlNodeToJaninoExpression(
                                            exprNode, udfDescriptors, columnNameMap),
                                    originalColumnNames,
                                    columnNameMap);
                }
            }
            // ... or an existing column's name identifier.
            else if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                projectionColumn =
                        resolveProjectionColumnFromIdentifier(
                                relDataType,
                                originalColumnMap,
                                columnName,
                                columnName,
                                supportedMetadataColumns);
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
            // Projection columns comes later could override previous ones.
            String projectionColumnName = projectionColumn.getColumnName();
            if (addedProjectionColumnNames.containsKey(projectionColumnName)) {
                // If we already have one column with identical name, replace it
                projectionColumns.set(
                        addedProjectionColumnNames.get(projectionColumnName), projectionColumn);
            } else {
                // Otherwise, append it at the end. Don't forget to set the index!
                projectionColumns.add(projectionColumn);
                addedProjectionColumnNames.put(projectionColumnName, projectionColumns.size() - 1);
            }
        }
        return projectionColumns;
    }

    /**
     * Create a projection column from a simple identifier node (could be an upstream physical
     * column or a metadata column).
     */
    public static ProjectionColumn resolveProjectionColumnFromIdentifier(
            RelDataType relDataType,
            Map<String, Column> originalColumnMap,
            String identifier,
            String projectedColumnName,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        Map<String, String> columnNameMap =
                Collections.singletonMap(identifier, MAPPED_SINGLE_COLUMN_NAME);
        if (isMetadataColumn(identifier, supportedMetadataColumns)) {
            // For a metadata column, we simply generate a projection column with the same
            return ProjectionColumn.ofCalculated(
                    projectedColumnName,
                    // Metadata columns should never be null
                    DataTypeConverter.convertCalciteRelDataTypeToDataType(relDataType).notNull(),
                    identifier,
                    columnNameMap.get(identifier),
                    Collections.singletonList(identifier),
                    columnNameMap);
        }

        Preconditions.checkArgument(
                originalColumnMap.containsKey(identifier),
                "Referenced column %s is not present in original table.",
                identifier);

        Column column = originalColumnMap.get(identifier);
        if (Objects.equals(identifier, projectedColumnName)) {
            return ProjectionColumn.ofForwarded(column, MAPPED_SINGLE_COLUMN_NAME);
        } else {
            return ProjectionColumn.ofAliased(
                    column, projectedColumnName, MAPPED_SINGLE_COLUMN_NAME);
        }
    }

    public static String translateFilterExpressionToJaninoExpression(
            String filterExpression,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        if (isNullOrWhitespaceOnly(filterExpression)) {
            return "";
        }
        SqlSelect sqlSelect = TransformParser.parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return "";
        }
        SqlNode where = sqlSelect.getWhere();
        return JaninoCompiler.translateSqlNodeToJaninoExpression(
                where, udfDescriptors, columnNameMap);
    }

    public static List<String> parseComputedColumnNames(
            String projection, SupportedMetadataColumn[] supportedMetadataColumns) {
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
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall);
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                String columnName = sqlNode.toString();
                if (isMetadataColumn(columnName, supportedMetadataColumns)
                        && !columnNames.contains(columnName)) {
                    columnNames.add(columnName);
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

    private static List<Column> copyFillMetadataColumn(
            List<Column> columns, SupportedMetadataColumn[] supportedMetadataColumns) {
        // Add metaColumn for SQLValidator.validate
        List<Column> columnsWithMetadata = new ArrayList<>(columns);
        METADATA_COLUMNS.stream()
                .map(col -> Column.physicalColumn(col.f0, col.f1))
                .forEach(columnsWithMetadata::add);
        Stream.of(supportedMetadataColumns)
                .map(sCol -> Column.physicalColumn(sCol.getName(), sCol.getType()))
                .forEach(columnsWithMetadata::add);
        return columnsWithMetadata;
    }

    private static boolean isMetadataColumn(
            String columnName, SupportedMetadataColumn[] supportedMetadataColumns) {
        return METADATA_COLUMNS.stream().anyMatch(col -> col.f0.equals(columnName))
                || Stream.of(supportedMetadataColumns)
                        .anyMatch(col -> col.getName().equals(columnName));
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

    public static boolean hasAsterisk(@Nullable String projection) {
        if (isNullOrWhitespaceOnly(projection)) {
            // Providing an empty projection expression is equivalent to writing `*` explicitly.
            return true;
        }
        return parseProjectionExpression(projection).getOperandList().stream()
                .anyMatch(TransformParser::hasAsterisk);
    }

    private static boolean hasAsterisk(SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            return ((SqlIdentifier) sqlNode).isStar();
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            return sqlBasicCall.getOperandList().stream().anyMatch(TransformParser::hasAsterisk);
        } else if (sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            return sqlNodeList.getList().stream().anyMatch(TransformParser::hasAsterisk);
        } else {
            return false;
        }
    }

    public static Map<String, String> generateColumnNameMap(List<String> originalColumnNames) {
        int i = 0;
        Map<String, String> columnNameMap = new HashMap<>();
        for (String columnName : originalColumnNames) {
            if (!columnNameMap.containsKey(columnName)) {
                columnNameMap.put(columnName, MAPPED_COLUMN_NAME_PREFIX + i);
                i++;
            }
        }
        return columnNameMap;
    }
}
