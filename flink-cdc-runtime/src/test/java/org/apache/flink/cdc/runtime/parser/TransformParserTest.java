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

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(
                "`CONCAT`(`id`, `order_id`) AS `uniq_id`, *", parse.getSelectList().toString());
        Assert.assertEquals("`uniq_id` > 10 AND `id` IS NOT NULL", parse.getWhere().toString());
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
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(sqlStdOperatorTable, transformSqlOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(parse);
        Assert.assertEquals(
                "SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`",
                parse.getSelectList().toString());
        Assert.assertEquals("`tb`.`id` IS NOT NULL", parse.getWhere().toString());
        Assert.assertEquals(
                "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                        + "FROM `default_schema`.`tb` AS `tb`\n"
                        + "WHERE `tb`.`id` IS NOT NULL",
                validateSqlNode.toString().replaceAll("\r\n", "\n"));
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
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(sqlStdOperatorTable, transformSqlOperatorTable),
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
        Assert.assertEquals(
                "SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`",
                parse.getSelectList().toString());
        Assert.assertEquals("`tb`.`id` IS NOT NULL", parse.getWhere().toString());
        Assert.assertEquals(
                "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                        + "FROM `default_schema`.`tb` AS `tb`\n"
                        + "WHERE `tb`.`id` IS NOT NULL",
                validateSqlNode.toString().replaceAll("\r\n", "\n"));
    }

    @Test
    public void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                TransformParser.parseComputedColumnNames("CONCAT(id, order_id) as uniq_id, *");
        Assert.assertEquals(new String[] {"uniq_id"}, computedColumnNames.toArray());
    }

    @Test
    public void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                TransformParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assert.assertEquals(new String[] {"uniq_id", "id"}, computedColumnNames.toArray());
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
                "id = CURRENT_TIMESTAMP",
                "valueEquals(id, currentTimestamp(__epoch_time__, __time_zone__))");
        testFilterExpression("NOW()", "now(__epoch_time__, __time_zone__)");
        testFilterExpression("YEAR(dt)", "year(dt)");
        testFilterExpression("QUARTER(dt)", "quarter(dt)");
        testFilterExpression("MONTH(dt)", "month(dt)");
        testFilterExpression("WEEK(dt)", "week(dt)");
        testFilterExpression("DATE_FORMAT(dt,'yyyy-MM-dd')", "dateFormat(dt, \"yyyy-MM-dd\")");
        testFilterExpression("TO_DATE(dt, 'yyyy-MM-dd')", "toDate(dt, \"yyyy-MM-dd\")");
        testFilterExpression("TO_TIMESTAMP(dt)", "toTimestamp(dt)");
        testFilterExpression("TIMESTAMP_DIFF('DAY', dt1, dt2)", "timestampDiff(\"DAY\", dt1, dt2)");
        testFilterExpression("IF(a>b,a,b)", "a > b ? a : b");
        testFilterExpression("NULLIF(a,b)", "nullif(a, b)");
        testFilterExpression("COALESCE(a,b,c)", "coalesce(a, b, c)");
        testFilterExpression("id + 2", "id + 2");
        testFilterExpression("id - 2", "id - 2");
        testFilterExpression("id * 2", "id * 2");
        testFilterExpression("id / 2", "id / 2");
        testFilterExpression("id % 2", "id % 2");
        testFilterExpression("a < b", "a < b");
        testFilterExpression("a <= b", "a <= b");
        testFilterExpression("a > b", "a > b");
        testFilterExpression("a >= b", "a >= b");
        testFilterExpression("__table_name__ = 'tb'", "valueEquals(__table_name__, \"tb\")");
        testFilterExpression("__schema_name__ = 'tb'", "valueEquals(__schema_name__, \"tb\")");
        testFilterExpression(
                "__namespace_name__ = 'tb'", "valueEquals(__namespace_name__, \"tb\")");
        testFilterExpression("upper(lower(id))", "upper(lower(id))");
        testFilterExpression(
                "abs(uniq_id) > 10 and id is not null", "abs(uniq_id) > 10 && null != id");
        testFilterExpression(
                "case id when 1 then 'a' when 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
        testFilterExpression(
                "case when id = 1 then 'a' when id = 2 then 'b' else 'c' end",
                "(valueEquals(id, 1) ? \"a\" : valueEquals(id, 2) ? \"b\" : \"c\")");
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(expression);
        Assert.assertEquals(expressionExpect, janinoExpression);
    }
}
