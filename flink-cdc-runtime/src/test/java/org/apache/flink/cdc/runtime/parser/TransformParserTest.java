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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSchemaFactory;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSqlOperatorTable;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Unit tests for the {@link TransformParser}. */
class TransformParserTest {

    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("order_id", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testCalciteParser() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select CONCAT(id, order_id) as uniq_id, * from tb where uniq_id > 10 and id is not null");
        Assertions.assertThat(parse.getSelectList())
                .hasToString("`CONCAT`(`id`, `order_id`) AS `uniq_id`, *");

        Assertions.assertThat(parse.getWhere()).hasToString("`uniq_id` > 10 AND `id` IS NOT NULL");
    }

    @Test
    void testTransformCalciteValidate() {
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

        Assertions.assertThat(parse.getSelectList())
                .hasToString("SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`");

        Assertions.assertThat(parse.getWhere()).hasToString("`tb`.`id` IS NOT NULL");

        Assertions.assertThat(validateSqlNode.toString().replaceAll("\r\n", "\n"))
                .isEqualTo(
                        "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                                + "FROM `default_schema`.`tb` AS `tb`\n"
                                + "WHERE `tb`.`id` IS NOT NULL");
    }

    @Test
    void testCalciteRelNode() {
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

        Assertions.assertThat(parse.getSelectList())
                .hasToString("SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`");

        Assertions.assertThat(parse.getWhere()).hasToString("`tb`.`id` IS NOT NULL");

        Assertions.assertThat(validateSqlNode.toString().replaceAll("\r\n", "\n"))
                .isEqualTo(
                        "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                                + "FROM `default_schema`.`tb` AS `tb`\n"
                                + "WHERE `tb`.`id` IS NOT NULL");
    }

    @Test
    void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                TransformParser.parseComputedColumnNames(
                        "CONCAT(id, order_id) as uniq_id, *", new SupportedMetadataColumn[0]);

        Assertions.assertThat(computedColumnNames.toArray()).isEqualTo(new String[] {"uniq_id"});
    }

    @Test
    void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                TransformParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assertions.assertThat(computedColumnNames.toArray())
                .isEqualTo(new String[] {"uniq_id", "id"});
    }

    @Test
    void testTranslateFilterToJaninoExpression() {
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
        testFilterExpression(
                "DATE_FORMAT(dt,'yyyy-MM-dd')", "dateFormat(dt, \"yyyy-MM-dd\", __time_zone__)");
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
                                    "TIMESTAMPDIFF(SECONDS, dt1, dt2)",
                                    Collections.emptyList(),
                                    Collections.emptyMap());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage("Statements can not be parsed.");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPDIFF(QUARTER, dt1, dt2)",
                                    Collections.emptyList(),
                                    Collections.emptyMap());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage(
                        "Unsupported time interval unit in timestamp diff function: \"QUARTER\"");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPADD(SECONDS, dt1, dt2)",
                                    Collections.emptyList(),
                                    Collections.emptyMap());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage("Statements can not be parsed.");
        Assertions.assertThatThrownBy(
                        () -> {
                            TransformParser.translateFilterExpressionToJaninoExpression(
                                    "TIMESTAMPADD(QUARTER, dt1, dt2)",
                                    Collections.emptyList(),
                                    Collections.emptyMap());
                        })
                .isExactlyInstanceOf(ParseException.class)
                .hasMessage(
                        "Unsupported time interval unit in timestamp add function: \"QUARTER\"");
    }

    @Test
    void testGenerateProjectionColumns() {
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
                        Column.physicalColumn("height", DataTypes.DOUBLE(), "height"),
                        Column.physicalColumn("op_type", DataTypes.TINYINT(), "op_type"));

        List<ProjectionColumn> result =
                TransformParser.generateProjectionColumns(
                        "id, upper(name) as name, age + 1 as newage, createTime as newCreateTime, address as newAddress, deposit as deposits, weight / (height * height) as bmi",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0]);

        List<String> expected =
                Arrays.asList(
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='$0', originalColumnNames=[id], columnNameMap={id=$0}}",
                        "ProjectionColumn{column=`name` STRING, expression='UPPER(`TB`.`name`)', scriptExpression='upper($0)', originalColumnNames=[name], columnNameMap={name=$0}}",
                        "ProjectionColumn{column=`newage` INT, expression='`TB`.`age` + 1', scriptExpression='$0 + 1', originalColumnNames=[age], columnNameMap={age=$0}}",
                        "ProjectionColumn{column=`newCreateTime` TIMESTAMP(3) 'newCreateTime', expression='createTime', scriptExpression='$0', originalColumnNames=[createTime], columnNameMap={createTime=$0}}",
                        "ProjectionColumn{column=`newAddress` VARCHAR(50) 'newAddress', expression='address', scriptExpression='$0', originalColumnNames=[address], columnNameMap={address=$0}}",
                        "ProjectionColumn{column=`deposits` DECIMAL(10, 2) 'deposit', expression='deposit', scriptExpression='$0', originalColumnNames=[deposit], columnNameMap={deposit=$0}}",
                        "ProjectionColumn{column=`bmi` DOUBLE, expression='`TB`.`weight` / (`TB`.`height` * `TB`.`height`)', scriptExpression='$0 / $1 * $1', originalColumnNames=[weight, height, height], columnNameMap={weight=$0, height=$1}}");
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");

        List<ProjectionColumn> metadataResult =
                TransformParser.generateProjectionColumns(
                        "*, __namespace_name__, __schema_name__, __table_name__, __data_event_type__ AS op_type",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0]);

        List<String> metadataExpected =
                Arrays.asList(
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='$0', originalColumnNames=[id], columnNameMap={id=$0}}",
                        "ProjectionColumn{column=`name` STRING 'name', expression='name', scriptExpression='$0', originalColumnNames=[name], columnNameMap={name=$0}}",
                        "ProjectionColumn{column=`age` INT 'age', expression='age', scriptExpression='$0', originalColumnNames=[age], columnNameMap={age=$0}}",
                        "ProjectionColumn{column=`createTime` TIMESTAMP(3) 'newCreateTime', expression='createTime', scriptExpression='$0', originalColumnNames=[createTime], columnNameMap={createTime=$0}}",
                        "ProjectionColumn{column=`address` VARCHAR(50) 'newAddress', expression='address', scriptExpression='$0', originalColumnNames=[address], columnNameMap={address=$0}}",
                        "ProjectionColumn{column=`deposit` DECIMAL(10, 2) 'deposit', expression='deposit', scriptExpression='$0', originalColumnNames=[deposit], columnNameMap={deposit=$0}}",
                        "ProjectionColumn{column=`weight` DOUBLE 'weight', expression='weight', scriptExpression='$0', originalColumnNames=[weight], columnNameMap={weight=$0}}",
                        "ProjectionColumn{column=`height` DOUBLE 'height', expression='height', scriptExpression='$0', originalColumnNames=[height], columnNameMap={height=$0}}",
                        "ProjectionColumn{column=`op_type` STRING NOT NULL, expression='__data_event_type__', scriptExpression='$0', originalColumnNames=[__data_event_type__], columnNameMap={__data_event_type__=$0}}",
                        "ProjectionColumn{column=`__namespace_name__` STRING NOT NULL, expression='__namespace_name__', scriptExpression='$0', originalColumnNames=[__namespace_name__], columnNameMap={__namespace_name__=$0}}",
                        "ProjectionColumn{column=`__schema_name__` STRING NOT NULL, expression='__schema_name__', scriptExpression='$0', originalColumnNames=[__schema_name__], columnNameMap={__schema_name__=$0}}",
                        "ProjectionColumn{column=`__table_name__` STRING NOT NULL, expression='__table_name__', scriptExpression='$0', originalColumnNames=[__table_name__], columnNameMap={__table_name__=$0}}");
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
                        "ProjectionColumn{column=`id` INT 'id', expression='id', scriptExpression='$0', originalColumnNames=[id], columnNameMap={id=$0}}",
                        "ProjectionColumn{column=`name2` STRING, expression='UPPER(`TB`.`name`)', scriptExpression='upper($0)', originalColumnNames=[name], columnNameMap={name=$0}}",
                        "ProjectionColumn{column=`sex2` STRING, expression='UPPER(`TB`.`sex`)', scriptExpression='upper($0)', originalColumnNames=[sex], columnNameMap={sex=$0}}",
                        "ProjectionColumn{column=`address2` BINARY(50), expression='CASE WHEN `TB`.`address` IS NOT NULL THEN `TB`.`address` ELSE `TB`.`address` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[address, address, address], columnNameMap={address=$0}}",
                        "ProjectionColumn{column=`phone2` VARBINARY(50), expression='CASE WHEN `TB`.`phone` IS NOT NULL THEN `TB`.`phone` ELSE `TB`.`phone` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[phone, phone, phone], columnNameMap={phone=$0}}",
                        "ProjectionColumn{column=`deposit2` DECIMAL(10, 2), expression='CASE WHEN `TB`.`deposit` IS NOT NULL THEN `TB`.`deposit` ELSE `TB`.`deposit` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[deposit, deposit, deposit], columnNameMap={deposit=$0}}",
                        "ProjectionColumn{column=`birthday2` TIMESTAMP(3), expression='CASE WHEN `TB`.`birthday` IS NOT NULL THEN `TB`.`birthday` ELSE `TB`.`birthday` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[birthday, birthday, birthday], columnNameMap={birthday=$0}}",
                        "ProjectionColumn{column=`birthday_ltz2` TIMESTAMP_LTZ(3), expression='CASE WHEN `TB`.`birthday_ltz` IS NOT NULL THEN `TB`.`birthday_ltz` ELSE `TB`.`birthday_ltz` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[birthday_ltz, birthday_ltz, birthday_ltz], columnNameMap={birthday_ltz=$0}}",
                        "ProjectionColumn{column=`update_time2` TIME(3), expression='CASE WHEN `TB`.`update_time` IS NOT NULL THEN `TB`.`update_time` ELSE `TB`.`update_time` END', scriptExpression='(null != $0 ? $0 : $0)', originalColumnNames=[update_time, update_time, update_time], columnNameMap={update_time=$0}}");
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");
    }

    @Test
    void testGenerateReferencedColumns() {
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
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");

        // calculated columns must use AS to provide an alias name
        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.generateReferencedColumns(
                                        "id, 1 + 1", null, testColumns))
                .isExactlyInstanceOf(ParseException.class);
    }

    @Test
    void testTranslateUdfFilterToJaninoExpression() {
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
    public void testTranslateUdfFilterToJaninoExpressionWithColumnNameMap() {
        Map<String, String> columnNameMap = new HashMap<>();
        columnNameMap.put("a", "$0");
        columnNameMap.put("b", "$1");
        columnNameMap.put("a-b", "$2");

        testFilterExpressionWithUdf(
                "format(upper(a))",
                "__instanceOfFormatFunctionClass.eval(upper($0))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "format(lower(b))",
                "__instanceOfFormatFunctionClass.eval(lower($1))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "format(concat(a,b))",
                "__instanceOfFormatFunctionClass.eval(concat($0, $1))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "format(SUBSTR(`a-b`,1))",
                "__instanceOfFormatFunctionClass.eval(substr($2, 1))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "typeof(`a-b` like '^[a-zA-Z]')",
                "__instanceOfTypeOfFunctionClass.eval(like($2, \"^[a-zA-Z]\"))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "typeof(`a-b` not like '^[a-zA-Z]')",
                "__instanceOfTypeOfFunctionClass.eval(notLike($2, \"^[a-zA-Z]\"))",
                columnNameMap);
        testFilterExpressionWithUdf(
                "typeof(a-b-`a-b`)",
                "__instanceOfTypeOfFunctionClass.eval($0 - $1 - $2)",
                columnNameMap);
        testFilterExpressionWithUdf(
                "typeof(a-b-2)",
                "__instanceOfTypeOfFunctionClass.eval($0 - $1 - 2)",
                columnNameMap);
        testFilterExpressionWithUdf(
                "addone(addone(`a-b`)) > 4 OR typeof(a-b) <> 'bool' AND format('from %s to %s is %s', 'a', 'z', 'lie') <> ''",
                "greaterThan(__instanceOfAddOneFunctionClass.eval(__instanceOfAddOneFunctionClass.eval($2)), 4) || !valueEquals(__instanceOfTypeOfFunctionClass.eval($0 - $1), \"bool\") && !valueEquals(__instanceOfFormatFunctionClass.eval(\"from %s to %s is %s\", \"a\", \"z\", \"lie\"), \"\")",
                columnNameMap);
        testFilterExpressionWithUdf(
                "ADDONE(ADDONE(`a-b`)) > 4 OR TYPEOF(a-b) <> 'bool' AND FORMAT('from %s to %s is %s', 'a', 'z', 'lie') <> ''",
                "greaterThan(__instanceOfAddOneFunctionClass.eval(__instanceOfAddOneFunctionClass.eval($2)), 4) || !valueEquals(__instanceOfTypeOfFunctionClass.eval($0 - $1), \"bool\") && !valueEquals(__instanceOfFormatFunctionClass.eval(\"from %s to %s is %s\", \"a\", \"z\", \"lie\"), \"\")",
                columnNameMap);
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
                                        "id > 9223372036854775808",
                                        Collections.emptyList(),
                                        Collections.emptyMap()))
                .isExactlyInstanceOf(CalciteContextException.class)
                .hasMessageContaining("Numeric literal '9223372036854775808' out of range");

        Assertions.assertThatThrownBy(
                        () ->
                                TransformParser.translateFilterExpressionToJaninoExpression(
                                        "id < -9223372036854775809",
                                        Collections.emptyList(),
                                        Collections.emptyMap()))
                .isExactlyInstanceOf(CalciteContextException.class)
                .hasMessageContaining("Numeric literal '-9223372036854775809' out of range");
    }

    @Test
    public void testProjectionColumnsWithColumnNameMap() {
        List<Column> testColumns =
                Arrays.asList(
                        Column.physicalColumn("a", DataTypes.INT(), "a"),
                        Column.physicalColumn("b", DataTypes.INT(), "b"),
                        Column.physicalColumn("a-b", DataTypes.DOUBLE(), "`a-b`"));

        List<ProjectionColumn> result =
                TransformParser.generateProjectionColumns(
                        "a, b, a-b as c, `a-b`, `a-b` AS d, `a-b`-1 AS e, a-b+`a-b` AS f, `test-meta-col`, `test-meta-col`-a-b AS g",
                        testColumns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[] {new TestMetadataColumn()});

        List<String> expected =
                Arrays.asList(
                        "ProjectionColumn{column=`a` INT 'a', expression='a', scriptExpression='$0', originalColumnNames=[a], columnNameMap={a=$0}}",
                        "ProjectionColumn{column=`b` INT 'b', expression='b', scriptExpression='$0', originalColumnNames=[b], columnNameMap={b=$0}}",
                        "ProjectionColumn{column=`c` INT, expression='`TB`.`a` - `TB`.`b`', scriptExpression='$0 - $1', originalColumnNames=[a, b], columnNameMap={a=$0, b=$1}}",
                        "ProjectionColumn{column=`a-b` DOUBLE '`a-b`', expression='a-b', scriptExpression='$0', originalColumnNames=[a-b], columnNameMap={a-b=$0}}",
                        "ProjectionColumn{column=`d` DOUBLE '`a-b`', expression='a-b', scriptExpression='$0', originalColumnNames=[a-b], columnNameMap={a-b=$0}}",
                        "ProjectionColumn{column=`e` DOUBLE, expression='`TB`.`a-b` - 1', scriptExpression='$0 - 1', originalColumnNames=[a-b], columnNameMap={a-b=$0}}",
                        "ProjectionColumn{column=`f` DOUBLE, expression='`TB`.`a` - `TB`.`b` + `TB`.`a-b`', scriptExpression='$0 - $1 + $2', originalColumnNames=[a, b, a-b], columnNameMap={a=$0, b=$1, a-b=$2}}",
                        "ProjectionColumn{column=`test-meta-col` INT NOT NULL, expression='test-meta-col', scriptExpression='$0', originalColumnNames=[test-meta-col], columnNameMap={test-meta-col=$0}}",
                        "ProjectionColumn{column=`g` INT, expression='`TB`.`test-meta-col` - `TB`.`a` - `TB`.`b`', scriptExpression='$0 - $1 - $2', originalColumnNames=[test-meta-col, a, b], columnNameMap={a=$1, b=$2, test-meta-col=$0}}");
        Assertions.assertThat(result).hasToString("[" + String.join(", ", expected) + "]");
    }

    private static final String[] UNICODE_STRINGS = {
        "ascii test!?",
        "大五",
        "测试数据",
        "ひびぴ",
        "죠주쥬",
        "ÀÆÉ",
        "ÓÔŐÖ",
        "αβγδε",
        "בבקשה",
        "твой",
        "ภาษาไทย",
        "piedzimst brīvi"
    };

    @Test
    void testParsingExpressionWithUnicodeLiterals() {
        List<Column> columns =
                Arrays.asList(
                        Column.physicalColumn("a", DataTypes.STRING(), "a"),
                        Column.physicalColumn("b", DataTypes.INT(), "b"));

        for (String unicodeString : UNICODE_STRINGS) {
            Assertions.assertThat(
                            TransformParser.generateProjectionColumns(
                                    "a, b, a = '{UNICODE_STRING}' AS c1, a <> '{UNICODE_STRING}' AS c2, b = '{UNICODE_STRING}' AS c3, b <> '{UNICODE_STRING}' AS c4"
                                            .replace("{UNICODE_STRING}", unicodeString),
                                    columns,
                                    Collections.emptyList(),
                                    new SupportedMetadataColumn[] {}))
                    .map(ProjectionColumn::getScriptExpression)
                    .containsExactly(
                            "$0",
                            "$0",
                            "valueEquals($0, \"" + unicodeString + "\")",
                            "!valueEquals($0, \"" + unicodeString + "\")",
                            "valueEquals($0, castToInteger(\"" + unicodeString + "\"))",
                            "!valueEquals($0, castToInteger(\"" + unicodeString + "\"))");

            testFilterExpression(
                    "a = '" + unicodeString + "'", "valueEquals(a, \"" + unicodeString + "\")");
            testFilterExpression(
                    "a <> '" + unicodeString + "'", "!valueEquals(a, \"" + unicodeString + "\")");
        }
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        expression, Collections.emptyList(), Collections.emptyMap());
        Assertions.assertThat(janinoExpression).isEqualTo(expressionExpect);
    }

    private void testFilterExpressionWithUdf(String expression, String expressionExpect) {
        testFilterExpressionWithUdf(expression, expressionExpect, Collections.emptyMap());
    }

    private void testFilterExpressionWithUdf(
            String expression, String expressionExpect, Map<String, String> columnNameMap) {
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
                                        "org.apache.flink.cdc.udf.examples.java.TypeOfFunctionClass")),
                        columnNameMap);
        Assertions.assertThat(janinoExpression).isEqualTo(expressionExpect);
    }

    /** Test metadata column. */
    private static class TestMetadataColumn implements SupportedMetadataColumn {
        @Override
        public String getName() {
            // Column name contains '-', which can be used to test column name map.
            return "test-meta-col";
        }

        @Override
        public DataType getType() {
            return DataTypes.INT();
        }

        @Override
        public Class<?> getJavaClass() {
            return Integer.class;
        }

        @Override
        public Object read(Map<String, String> metadata) {
            return 0;
        }
    }
}
