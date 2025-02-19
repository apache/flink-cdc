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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSchemaFactory;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSqlOperatorTable;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Unit tests for the {@link TransformParser}. */
public class TransformParserTest {

    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("order_id", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    public void testCalciteParser() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select CONCAT(id, order_id) as uniq_id, * from tb where uniq_id > 10 and id is not null");
        Assertions.assertThat(parse.getSelectList().toString())
                .isEqualTo("`CONCAT`(`id`, `order_id`) AS `uniq_id`, *");

        Assertions.assertThat(parse.getWhere().toString())
                .isEqualTo("`uniq_id` > 10 AND `id` IS NOT NULL");
    }

    @Test
    public void testTransformCalciteValidate() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                TransformSchemaFactory.INSTANCE.create(
                        rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        transformSqlOperatorTable,
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(parse);

        Assertions.assertThat(parse.getSelectList().toString())
                .isEqualTo("SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`");

        Assertions.assertThat(parse.getWhere().toString()).isEqualTo("`tb`.`id` IS NOT NULL");

        Assertions.assertThat(validateSqlNode.toString().replaceAll("\r\n", "\n"))
                .isEqualTo(
                        "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                                + "FROM `default_schema`.`tb` AS `tb`\n"
                                + "WHERE `tb`.`id` IS NOT NULL");
    }

    @Test
    public void testCalciteRelNode() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                TransformSchemaFactory.INSTANCE.create(
                        rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        transformSqlOperatorTable,
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(parse);
        RexBuilder rexBuilder = new RexBuilder(factory);
        HepProgramBuilder builder = new HepProgramBuilder();
        HepPlanner planner = new HepPlanner(builder.build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter.config().withTrimUnusedFields(false);
        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        null,
                        validator,
                        calciteCatalogReader,
                        cluster,
                        StandardConvertletTable.INSTANCE,
                        config);
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, true);
        relRoot = relRoot.withRel(sqlToRelConverter.flattenTypes(relRoot.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        relRoot = relRoot.withRel(RelDecorrelator.decorrelateQuery(relRoot.rel, relBuilder));
        RelNode relNode = relRoot.rel;

        Assertions.assertThat(parse.getSelectList().toString())
                .isEqualTo("SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`");

        Assertions.assertThat(parse.getWhere().toString()).isEqualTo("`tb`.`id` IS NOT NULL");

        Assertions.assertThat(validateSqlNode.toString().replaceAll("\r\n", "\n"))
                .isEqualTo(
                        "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                                + "FROM `default_schema`.`tb` AS `tb`\n"
                                + "WHERE `tb`.`id` IS NOT NULL");
    }

    @Test
    public void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                TransformParser.parseComputedColumnNames(
                        "CONCAT(id, order_id) as uniq_id, *", new SupportedMetadataColumn[0]);

        Assertions.assertThat(computedColumnNames.toArray()).isEqualTo(new String[] {"uniq_id"});
    }

    @Test
    public void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                TransformParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assertions.assertThat(computedColumnNames.toArray())
                .isEqualTo(new String[] {"uniq_id", "id"});
    }

    @Test
    public void testTranslateFilterToJaninoExpression() {
        testFilterExpression("id is not null", "null != id");
        testFilterExpression("id is null", "null == id");
        testFilterExpression("id = 1 and uid = 2", "valueEquals(id, 1) && valueEquals(uid, 2)");
        testFilterExpression("id = 1 or id = 2", "valueEquals(id, 1) || valueEquals(id, 2)");
        testFilterExpression("not (id = 1)", "!valueEquals(id, 1)");
        testFilterExpression("id = '1'", "valueEquals(id, \"1\")");
        testFilterExpression("id <> '1'", "!valueEquals(id, \"1\")");
        testFilterExpression("d between d1 and d2", "betweenAsymmetric(d, d1, d2)");
        testFilterExpression("d not between d1 and d2", "notBetweenAsymmetric(d, d1, d2)");
        testFilterExpression("d in (d1, d2)", "in(d, d1, d2)");
        testFilterExpression("d not in (d1, d2)", "notIn(d, d1, d2)");
        testFilterExpression("id is false", "false == id");
        testFilterExpression("id is not false", "true == id");
        testFilterExpression("id is true", "true == id");
        testFilterExpression("id is not true", "false == id");
        testFilterExpression("a || b", "concat(a, b)");
        testFilterExpression("CHAR_LENGTH(id)", "charLength(id)");
        testFilterExpression("trim(id)", "trim(\"BOTH\", \" \", id)");
        testFilterExpression(
                "REGEXP_REPLACE(id, '[a-zA-Z]', '')", "regexpReplace(id, \"[a-zA-Z]\", \"\")");
        testFilterExpression("upper(id)", "upper(id)");
        testFilterExpression("lower(id)", "lower(id)");
        testFilterExpression("concat(a,b)", "concat(a, b)");
        testFilterExpression("SUBSTR(a,1)", "substr(a, 1)");
        testFilterExpression("id like '^[a-zA-Z]'", "like(id, \"^[a-zA-Z]\")");
        testFilterExpression("id not like '^[a-zA-Z]'", "notLike(id, \"^[a-zA-Z]\")");
        testFilterExpression("abs(2)", "abs(2)");
        testFilterExpression("ceil(2)", "ceil(2)");
        testFilterExpression("ceiling(2)", "ceil(2)");
        testFilterExpression("floor(2)", "floor(2)");
        testFilterExpression("round(2,2)", "round(2, 2)");
        testFilterExpression("uuid()", "uuid()");
        testFilterExpression(
                "id = LOCALTIME", "valueEquals(id, localtime(__epoch_time__, __time_zone__))");
        testFilterExpression(
                "id = LOCALTIMESTAMP",
                "valueEquals(id, localtimestamp(__epoch_time__, __time_zone__))");
        testFilterExpression(
                "id = CURRENT_TIME", "valueEquals(id, currentTime(__epoch_time__, __time_zone__))");
        testFilterExpression(
                "id = CURRENT_DATE", "valueEquals(id, currentDate(__epoch_time__, __time_zone__))");
        testFilterExpression(
                "id = CURRENT_TIMESTAMP", "valueEquals(id, currentTimestamp(__epoch_time__))");
        testFilterExpression("NOW()", "now(__epoch_time__)");
        testFilterExpression("FROM_UNIXTIME(44)", "fromUnixtime(44, __time_zone__)");
        testFilterExpression(
                "FROM_UNIXTIME(44, 'yyyy/MM/dd HH:mm:ss')",
                "fromUnixtime(44, \"yyyy/MM/dd HH:mm:ss\", __time_zone__)");
        testFilterExpression("UNIX_TIMESTAMP()", "unixTimestamp(__epoch_time__, __time_zone__)");
        testFilterExpression(
                "UNIX_TIMESTAMP('1970-01-01 08:00:01')",
                "unixTimestamp(\"1970-01-01 08:00:01\", __epoch_time__, __time_zone__)");
        testFilterExpression(
                "UNIX_TIMESTAMP('1970-01-01 08:00:01.001 +0800', 'yyyy-MM-dd HH:mm:ss.SSS X')",
                "unixTimestamp(\"1970-01-01 08:00:01.001 +0800\", \"yyyy-MM-dd HH:mm:ss.SSS X\", __epoch_time__, __time_zone__)");
        testFilterExpression("YEAR(dt)", "year(dt)");
        testFilterExpression("QUARTER(dt)", "quarter(dt)");
        testFilterExpression("MONTH(dt)", "month(dt)");
        testFilterExpression("WEEK(dt)", "week(dt)");
        testFilterExpression("DATE_FORMAT(dt,'yyyy-MM-dd')", "dateFormat(dt, \"yyyy-MM-dd\")");
        testFilterExpression(
                "TO_DATE(dt, 'yyyy-MM-dd')", "toDate(dt, \"yyyy-MM-dd\", __time_zone__)");
        testFilterExpression("TO_TIMESTAMP(dt)", "toTimestamp(dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('SECOND', dt1, dt2)",
                "timestampDiff(\"SECOND\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('second', dt1, dt2)",
                "timestampDiff(\"second\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('MINUTE', dt1, dt2)",
                "timestampDiff(\"MINUTE\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('minute', dt1, dt2)",
                "timestampDiff(\"minute\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('HOUR', dt1, dt2)",
                "timestampDiff(\"HOUR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('hour', dt1, dt2)",
                "timestampDiff(\"hour\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('DAY', dt1, dt2)",
                "timestampDiff(\"DAY\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('day', dt1, dt2)",
                "timestampDiff(\"day\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('MONTH', dt1, dt2)",
                "timestampDiff(\"MONTH\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('month', dt1, dt2)",
                "timestampDiff(\"month\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMP_DIFF('YEAR', dt1, dt2)",
                "timestampDiff(\"YEAR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestamp_diff('year', dt1, dt2)",
                "timestampDiff(\"year\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(SECOND, dt1, dt2)",
                "timestampdiff(\"SECOND\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(second, dt1, dt2)",
                "timestampdiff(\"SECOND\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(MINUTE, dt1, dt2)",
                "timestampdiff(\"MINUTE\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(minute, dt1, dt2)",
                "timestampdiff(\"MINUTE\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(HOUR, dt1, dt2)",
                "timestampdiff(\"HOUR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(hour, dt1, dt2)",
                "timestampdiff(\"HOUR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(DAY, dt1, dt2)", "timestampdiff(\"DAY\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(day, dt1, dt2)", "timestampdiff(\"DAY\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(MONTH, dt1, dt2)",
                "timestampdiff(\"MONTH\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(month, dt1, dt2)",
                "timestampdiff(\"MONTH\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPDIFF(YEAR, dt1, dt2)",
                "timestampdiff(\"YEAR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "timestampdiff(year, dt1, dt2)",
                "timestampdiff(\"YEAR\", dt1, dt2, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(SECOND, 1, dt)", "timestampadd(\"SECOND\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(second, 1, dt)", "timestampadd(\"SECOND\", 1, dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(MINUTE, 1, dt)", "timestampadd(\"MINUTE\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(minute, 1, dt)", "timestampadd(\"MINUTE\", 1, dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(HOUR, 1, dt)", "timestampadd(\"HOUR\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(hour, 1, dt)", "timestampadd(\"HOUR\", 1, dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(DAY, 1, dt)", "timestampadd(\"DAY\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(day, 1, dt)", "timestampadd(\"DAY\", 1, dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(MONTH, 1, dt)", "timestampadd(\"MONTH\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(month, 1, dt)", "timestampadd(\"MONTH\", 1, dt, __time_zone__)");
        testFilterExpression(
                "TIMESTAMPADD(YEAR, 1, dt)", "timestampadd(\"YEAR\", 1, dt, __time_zone__)");
        testFilterExpression(
                "timestampadd(year, 1, dt)", "timestampadd(\"YEAR\", 1, dt, __time_zone__)");
        testFilterExpression("IF(a>b,a,b)", "greaterThan(a, b) ? a : b");
        testFilterExpression("NULLIF(a,b)", "nullif(a, b)");
        testFilterExpression("COALESCE(a,b,c)", "coalesce(a, b, c)");
        testFilterExpression("id + 2", "id + 2");
        testFilterExpression("id - 2", "id - 2");
        testFilterExpression("id * 2", "id * 2");
        testFilterExpression("id / 2", "id / 2");
        testFilterExpression("id % 2", "id % 2");
        testFilterExpression("a < b", "lessThan(a, b)");
        testFilterExpression("a <= b", "lessThanOrEqual(a, b)");
        testFilterExpression("a > b", "greaterThan(a, b)");
        testFilterExpression("a >= b", "greaterThanOrEqual(a, b)");
        testFilterExpression("__table_name__ = 'tb'", "valueEquals(__table_name__, \"tb\")");
        testFilterExpression("__schema_name__ = 'tb'", "valueEquals(__schema_name__, \"tb\")");
        testFilterExpression(
                "__namespace_name__ = 'tb'", "valueEquals(__namespace_name__, \"tb\")");
        testFilterExpression("upper(lower(id))", "upper(lower(id))");
        testFilterExpression(
                "abs(uniq_id) > 10 and id is not null",
                "greaterThan(abs(uniq_id), 10) && null != id");
        testFilterExpression(
                "case id when 1 then 'a' when 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
        testFilterExpression(
                "case when id = 1 then 'a' when id = 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
        testFilterExpression(
                "case id when 1 then 'a' when 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
        testFilterExpression(
                "case when id = 1 then 'a' when id = 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
        testFilterExpression("cast(id||'0' as int)", "castToInteger(concat(id, \"0\"))");
        testFilterExpression("cast(1 as string)", "castToString(1)");
        testFilterExpression("cast(1 as boolean)", "castToBoolean(1)");
        testFilterExpression("cast(1 as tinyint)", "castToByte(1)");
        testFilterExpression("cast(1 as smallint)", "castToShort(1)");
        testFilterExpression("cast(1 as bigint)", "castToLong(1)");
        testFilterExpression("cast(1 as float)", "castToFloat(1)");
        testFilterExpression("cast(1 as double)", "castToDouble(1)");
        testFilterExpression("cast(1 as decimal)", "castToDecimalData(1, 10, 0)");
        testFilterExpression("cast(1 as char)", "castToString(1)");
        testFilterExpression("cast(1 as varchar)", "castToString(1)");
        testFilterExpression("cast(null as int)", "castToInteger(null)");
        testFilterExpression("cast(null as string)", "castToString(null)");
        testFilterExpression("cast(null as boolean)", "castToBoolean(null)");
        testFilterExpression("cast(null as tinyint)", "castToByte(null)");
        testFilterExpression("cast(null as smallint)", "castToShort(null)");
        testFilterExpression("cast(null as bigint)", "castToLong(null)");
        testFilterExpression("cast(null as float)", "castToFloat(null)");
        testFilterExpression("cast(null as double)", "castToDouble(null)");
        testFilterExpression("cast(null as decimal)", "castToDecimalData(null, 10, 0)");
        testFilterExpression("cast(null as char)", "castToString(null)");
        testFilterExpression("cast(null as varchar)", "castToString(null)");
        testFilterExpression(
                "cast(CURRENT_TIMESTAMP as TIMESTAMP)",
                "castToTimestamp(currentTimestamp(__epoch_time__), __time_zone__)");
        testFilterExpression("cast(dt as TIMESTAMP)", "castToTimestamp(dt, __time_zone__)");
    }

    @Test
    public void testTranslateFilterToJaninoExpressionError() {
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPDIFF(SECONDS, dt1, dt2)", Collections.emptyList());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage("Statements can not be parsed.");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPDIFF(QUARTER, dt1, dt2)", Collections.emptyList());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage(
                        "Unsupported time interval unit in timestamp diff function: \"QUARTER\"");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPADD(SECONDS, dt1, dt2)", Collections.emptyList());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage("Statements can not be parsed.");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPADD(QUARTER, dt1, dt2)", Collections.emptyList());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage(
                        "Unsupported time interval unit in timestamp add function: \"QUARTER\"");
    }

    @Test
    public void testGenerateProjectionColumns() {
        List<Column> testColumns =
                Arrays.asList(
                        Column.physicalColumn("id", DataTypes.INT(), "id"),
                        Column.physicalColumn("name", DataTypes.STRING(), "name"),
                        Column.physicalColumn("age", DataTypes.INT(), "age"),
                        Column.physicalColumn(
                                "createTime", DataTypes.TIMESTAMP(3), "newCreateTime"),
                        Column.physicalColumn("address", DataTypes.VARCHAR(50), "newAddress"),
                        Column.physicalColumn("deposit", DataTypes.DECIMAL(10, 2), "deposit"),
                        Column.physicalColumn("weight", DataTypes.DOUBLE(), "weight"),
                        Column.physicalColumn("height", DataTypes.DOUBLE(), "height"));

        List<ProjectionColumn> result =
                TransformParser.generateProjectionColumns(
                        "id, upper(name) as name, age + 1 as newage, createTime as newCreateTime, address as newAddress, deposit as deposits, weight / (height * height) as bmi",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0]);

        List<String> expected =
                Arrays.asList(
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='id', originalColumnNames=[id], transformExpressionKey=null}",
                        "ProjectionColumn{column=`name` STRING, expression='UPPER(`TB`.`name`)', scriptExpression='upper(name)', originalColumnNames=[name], transformExpressionKey=null}",
                        "ProjectionColumn{column=`newage` INT, expression='`TB`.`age` + 1', scriptExpression='age + 1', originalColumnNames=[age], transformExpressionKey=null}",
                        "ProjectionColumn{column=`newCreateTime` TIMESTAMP(3) 'newCreateTime', expression='createTime', scriptExpression='createTime', originalColumnNames=[createTime], transformExpressionKey=null}",
                        "ProjectionColumn{column=`newAddress` VARCHAR(50) 'newAddress', expression='address', scriptExpression='address', originalColumnNames=[address], transformExpressionKey=null}",
                        "ProjectionColumn{column=`deposits` DECIMAL(10, 2) 'deposit', expression='deposit', scriptExpression='deposit', originalColumnNames=[deposit], transformExpressionKey=null}",
                        "ProjectionColumn{column=`bmi` DOUBLE, expression='`TB`.`weight` / (`TB`.`height` * `TB`.`height`)', scriptExpression='weight / height * height', originalColumnNames=[weight, height, height], transformExpressionKey=null}");
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");

        List<ProjectionColumn> metadataResult =
                TransformParser.generateProjectionColumns(
                        "*, __namespace_name__, __schema_name__, __table_name__",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0]);

        List<String> metadataExpected =
                Arrays.asList(
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='id', originalColumnNames=[id], transformExpressionKey=null}",
                        "ProjectionColumn{column=`name` STRING 'name', expression='name', scriptExpression='name', originalColumnNames=[name], transformExpressionKey=null}",
                        "ProjectionColumn{column=`age` INT 'age', expression='age', scriptExpression='age', originalColumnNames=[age], transformExpressionKey=null}",
                        "ProjectionColumn{column=`createTime` TIMESTAMP(3) 'newCreateTime', expression='createTime', scriptExpression='createTime', originalColumnNames=[createTime], transformExpressionKey=null}",
                        "ProjectionColumn{column=`address` VARCHAR(50) 'newAddress', expression='address', scriptExpression='address', originalColumnNames=[address], transformExpressionKey=null}",
                        "ProjectionColumn{column=`deposit` DECIMAL(10, 2) 'deposit', expression='deposit', scriptExpression='deposit', originalColumnNames=[deposit], transformExpressionKey=null}",
                        "ProjectionColumn{column=`weight` DOUBLE 'weight', expression='weight', scriptExpression='weight', originalColumnNames=[weight], transformExpressionKey=null}",
                        "ProjectionColumn{column=`height` DOUBLE 'height', expression='height', scriptExpression='height', originalColumnNames=[height], transformExpressionKey=null}",
                        "ProjectionColumn{column=`__namespace_name__` STRING NOT NULL, expression='__namespace_name__', scriptExpression='__namespace_name__', originalColumnNames=[__namespace_name__], transformExpressionKey=null}",
                        "ProjectionColumn{column=`__schema_name__` STRING NOT NULL, expression='__schema_name__', scriptExpression='__schema_name__', originalColumnNames=[__schema_name__], transformExpressionKey=null}",
                        "ProjectionColumn{column=`__table_name__` STRING NOT NULL, expression='__table_name__', scriptExpression='__table_name__', originalColumnNames=[__table_name__], transformExpressionKey=null}");
        Assertions.assertThat(metadataResult)
                .map(ProjectionColumn::toString)
                .containsExactlyElementsOf(metadataExpected);

        // calculated columns must use AS to provide an alias name
        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.generateProjectionColumns(
                                        "id, 1 + 1",
                                        testColumns,
                                        Collections.emptyList(),
                                        new SupportedMetadataColumn[0]))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unrecognized projection expression: 1 + 1. Should be <EXPR> AS <IDENTIFIER>");
    }

    @Test
    public void testGenerateProjectionColumnsWithPrecision() {
        List<Column> testColumns =
                Arrays.asList(
                        Column.physicalColumn("id", DataTypes.INT(), "id"),
                        Column.physicalColumn("name", DataTypes.VARCHAR(50), "name"),
                        Column.physicalColumn("sex", DataTypes.CHAR(1), "sex"),
                        Column.physicalColumn("address", DataTypes.BINARY(50), "address"),
                        Column.physicalColumn("phone", DataTypes.VARBINARY(50), "phone"),
                        Column.physicalColumn("deposit", DataTypes.DECIMAL(10, 2), "deposit"),
                        Column.physicalColumn("birthday", DataTypes.TIMESTAMP(3), "birthday"),
                        Column.physicalColumn(
                                "birthday_ltz", DataTypes.TIMESTAMP_LTZ(3), "birthday_ltz"),
                        Column.physicalColumn("update_time", DataTypes.TIME(3), "update_time"));

        List<ProjectionColumn> result =
                TransformParser.generateProjectionColumns(
                        "id, UPPER(name) as name2, UPPER(sex) as sex2, COALESCE(address,address) as address2, COALESCE(phone,phone) as phone2, COALESCE(deposit,deposit) as deposit2, COALESCE(birthday,birthday) as birthday2, COALESCE(birthday_ltz,birthday_ltz) as birthday_ltz2, COALESCE(update_time,update_time) as update_time2",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0]);

        List<String> expected =
                Arrays.asList(
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='id', originalColumnNames=[id], transformExpressionKey=null}",
                        "ProjectionColumn{column=`name2` STRING, expression='UPPER(`TB`.`name`)', scriptExpression='upper(name)', originalColumnNames=[name], transformExpressionKey=null}",
                        "ProjectionColumn{column=`sex2` STRING, expression='UPPER(`TB`.`sex`)', scriptExpression='upper(sex)', originalColumnNames=[sex], transformExpressionKey=null}",
                        "ProjectionColumn{column=`address2` BINARY(50), expression='CASE WHEN `TB`.`address` IS NOT NULL THEN `TB`.`address` ELSE `TB`.`address` END', scriptExpression='(null != address ? address : address)', originalColumnNames=[address, address, address], transformExpressionKey=null}",
                        "ProjectionColumn{column=`phone2` VARBINARY(50), expression='CASE WHEN `TB`.`phone` IS NOT NULL THEN `TB`.`phone` ELSE `TB`.`phone` END', scriptExpression='(null != phone ? phone : phone)', originalColumnNames=[phone, phone, phone], transformExpressionKey=null}",
                        "ProjectionColumn{column=`deposit2` DECIMAL(10, 2), expression='CASE WHEN `TB`.`deposit` IS NOT NULL THEN `TB`.`deposit` ELSE `TB`.`deposit` END', scriptExpression='(null != deposit ? deposit : deposit)', originalColumnNames=[deposit, deposit, deposit], transformExpressionKey=null}",
                        "ProjectionColumn{column=`birthday2` TIMESTAMP(3), expression='CASE WHEN `TB`.`birthday` IS NOT NULL THEN `TB`.`birthday` ELSE `TB`.`birthday` END', scriptExpression='(null != birthday ? birthday : birthday)', originalColumnNames=[birthday, birthday, birthday], transformExpressionKey=null}",
                        "ProjectionColumn{column=`birthday_ltz2` TIMESTAMP_LTZ(3), expression='CASE WHEN `TB`.`birthday_ltz` IS NOT NULL THEN `TB`.`birthday_ltz` ELSE `TB`.`birthday_ltz` END', scriptExpression='(null != birthday_ltz ? birthday_ltz : birthday_ltz)', originalColumnNames=[birthday_ltz, birthday_ltz, birthday_ltz], transformExpressionKey=null}",
                        "ProjectionColumn{column=`update_time2` TIME(3), expression='CASE WHEN `TB`.`update_time` IS NOT NULL THEN `TB`.`update_time` ELSE `TB`.`update_time` END', scriptExpression='(null != update_time ? update_time : update_time)', originalColumnNames=[update_time, update_time, update_time], transformExpressionKey=null}");
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");
    }

    @Test
    public void testGenerateReferencedColumns() {
        List<Column> testColumns =
                Arrays.asList(
                        Column.physicalColumn("id", DataTypes.INT(), "id"),
                        Column.physicalColumn("name", DataTypes.STRING(), "string"),
                        Column.physicalColumn("age", DataTypes.INT(), "age"),
                        Column.physicalColumn("address", DataTypes.STRING(), "address"),
                        Column.physicalColumn("weight", DataTypes.DOUBLE(), "weight"),
                        Column.physicalColumn("height", DataTypes.DOUBLE(), "height"),
                        Column.physicalColumn("birthday", DataTypes.DATE(), "birthday"));

        List<Column> result =
                TransformParser.generateReferencedColumns(
                        "id, upper(name) as name, age + 1 as newage, weight / (height * height) as bmi",
                        "bmi > 17 and char_length(address) > 10",
                        testColumns);

        List<String> expected =
                Arrays.asList(
                        "`id` INT 'id'",
                        "`name` STRING 'string'",
                        "`age` INT 'age'",
                        "`address` STRING 'address'",
                        "`weight` DOUBLE 'weight'",
                        "`height` DOUBLE 'height'");
        Assertions.assertThat(result.toString()).isEqualTo("[" + String.join(", ", expected) + "]");

        // calculated columns must use AS to provide an alias name
        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.generateReferencedColumns(
                                        "id, 1 + 1", null, testColumns))
                .isExactlyInstanceOf(ParseException.class);
    }

    @Test
    public void testNormalizeFilter() {
        Assertions.assertThat(TransformParser.normalizeFilter("a, b, c, d", "a > 0 and b > 0"))
                .isEqualTo("`a` > 0 AND `b` > 0");
        Assertions.assertThat(TransformParser.normalizeFilter("a, b, c, d", null)).isEqualTo(null);
        Assertions.assertThat(
                        TransformParser.normalizeFilter(
                                "abs(a) as cal_a, char_length(b) as cal_b, c, d",
                                "a > 4 and cal_a > 8 and cal_b < 17 and c != d"))
                .isEqualTo("`a` > 4 AND ABS(`a`) > 8 AND CHAR_LENGTH(`b`) < 17 AND `c` <> `d`");

        Assertions.assertThat(
                        TransformParser.normalizeFilter(
                                "x, y, z, 1 - x as u, 1 - y as v, 1 - z as w",
                                "concat(u, concat(v, concat(w, x), y), z) != 10"))
                .isEqualTo(
                        "`concat`(1 - `x`, `concat`(1 - `y`, `concat`(1 - `z`, `x`), `y`), `z`) <> 10");
    }

    @Test
    public void testTranslateUdfFilterToJaninoExpression() {
        testFilterExpressionWithUdf(
                "format(upper(id))", "__instanceOfFormatFunctionClass.eval(upper(id))");
        testFilterExpressionWithUdf(
                "format(lower(id))", "__instanceOfFormatFunctionClass.eval(lower(id))");
        testFilterExpressionWithUdf(
                "format(concat(a,b))", "__instanceOfFormatFunctionClass.eval(concat(a, b))");
        testFilterExpressionWithUdf(
                "format(SUBSTR(a,1))", "__instanceOfFormatFunctionClass.eval(substr(a, 1))");
        testFilterExpressionWithUdf(
                "typeof(id like '^[a-zA-Z]')",
                "__instanceOfTypeOfFunctionClass.eval(like(id, \"^[a-zA-Z]\"))");
        testFilterExpressionWithUdf(
                "typeof(id not like '^[a-zA-Z]')",
                "__instanceOfTypeOfFunctionClass.eval(notLike(id, \"^[a-zA-Z]\"))");
        testFilterExpressionWithUdf(
                "typeof(abs(2))", "__instanceOfTypeOfFunctionClass.eval(abs(2))");
        testFilterExpressionWithUdf(
                "typeof(ceil(2))", "__instanceOfTypeOfFunctionClass.eval(ceil(2))");
        testFilterExpressionWithUdf(
                "typeof(ceiling(2))", "__instanceOfTypeOfFunctionClass.eval(ceil(2))");
        testFilterExpressionWithUdf(
                "typeof(floor(2))", "__instanceOfTypeOfFunctionClass.eval(floor(2))");
        testFilterExpressionWithUdf(
                "typeof(round(2,2))", "__instanceOfTypeOfFunctionClass.eval(round(2, 2))");
        testFilterExpressionWithUdf(
                "typeof(id + 2)", "__instanceOfTypeOfFunctionClass.eval(id + 2)");
        testFilterExpressionWithUdf(
                "typeof(id - 2)", "__instanceOfTypeOfFunctionClass.eval(id - 2)");
        testFilterExpressionWithUdf(
                "typeof(id * 2)", "__instanceOfTypeOfFunctionClass.eval(id * 2)");
        testFilterExpressionWithUdf(
                "typeof(id / 2)", "__instanceOfTypeOfFunctionClass.eval(id / 2)");
        testFilterExpressionWithUdf(
                "typeof(id % 2)", "__instanceOfTypeOfFunctionClass.eval(id % 2)");
        testFilterExpressionWithUdf(
                "addone(addone(id)) > 4 OR typeof(id) <> 'bool' AND format('from %s to %s is %s', 'a', 'z', 'lie') <> ''",
                "greaterThan(__instanceOfAddOneFunctionClass.eval(__instanceOfAddOneFunctionClass.eval(id)), 4) || !valueEquals(__instanceOfTypeOfFunctionClass.eval(id), \"bool\") && !valueEquals(__instanceOfFormatFunctionClass.eval(\"from %s to %s is %s\", \"a\", \"z\", \"lie\"), \"\")");
        testFilterExpressionWithUdf(
                "ADDONE(ADDONE(id)) > 4 OR TYPEOF(id) <> 'bool' AND FORMAT('from %s to %s is %s', 'a', 'z', 'lie') <> ''",
                "greaterThan(__instanceOfAddOneFunctionClass.eval(__instanceOfAddOneFunctionClass.eval(id)), 4) || !valueEquals(__instanceOfTypeOfFunctionClass.eval(id), \"bool\") && !valueEquals(__instanceOfFormatFunctionClass.eval(\"from %s to %s is %s\", \"a\", \"z\", \"lie\"), \"\")");
    }

    @Test
    void testLargeNumericalLiterals() {
        // For literals within [-2147483648, 2147483647] range, plain Integers are OK
        testFilterExpression("id > 2147483647", "greaterThan(id, 2147483647)");
        testFilterExpression("id < -2147483648", "lessThan(id, -2147483648)");

        // For out-of-range literals, an extra `L` suffix is required
        testFilterExpression("id > 2147483648", "greaterThan(id, 2147483648L)");
        testFilterExpression("id > -2147483649", "greaterThan(id, -2147483649L)");
        testFilterExpression("id < 9223372036854775807", "lessThan(id, 9223372036854775807L)");
        testFilterExpression("id > -9223372036854775808", "greaterThan(id, -9223372036854775808L)");

        // But there's still a limit
        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.translateFilterExpressionToJaninoExpression(
                                        "id > 9223372036854775808", Collections.emptyList()))
                .isExactlyInstanceOf(CalciteContextException.class)
                .hasMessageContaining("Numeric literal '9223372036854775808' out of range");

        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.translateFilterExpressionToJaninoExpression(
                                        "id < -9223372036854775809", Collections.emptyList()))
                .isExactlyInstanceOf(CalciteContextException.class)
                .hasMessageContaining("Numeric literal '-9223372036854775809' out of range");
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        expression, Collections.emptyList());
        Assertions.assertThat(janinoExpression).isEqualTo(expressionExpect);
    }

    private void testFilterExpressionWithUdf(String expression, String expressionExpect) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        expression,
                        Arrays.asList(
                                new UserDefinedFunctionDescriptor(
                                        "format",
                                        "org.apache.flink.cdc.udf.examples.java.FormatFunctionClass"),
                                new UserDefinedFunctionDescriptor(
                                        "addone",
                                        "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass"),
                                new UserDefinedFunctionDescriptor(
                                        "typeof",
                                        "org.apache.flink.cdc.udf.examples.java.TypeOfFunctionClass")));
        Assertions.assertThat(janinoExpression).isEqualTo(expressionExpect);
    }
}
