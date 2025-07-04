---
title: "Transform"
weight: 5
type: docs
aliases:
  - /core-concept/transform/
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 定义
**Transform** 模块允许用户根据数据列对数据列进行删除和扩展。
更进一步的，这也也可以帮助用户在同步过程中过滤一些不需要的数据。

# 参数
为了定义一个transform规则，可以使用以下参数：

| 参数                        | 含义                                                                                | Optional/Required |
|---------------------------|-----------------------------------------------------------------------------------|-------------------|
| source-table              | Source table id, supports regular expressions                                     | required          |
| projection                | Projection rule, supports syntax similar to the select clause in SQL              | optional          |
| filter                    | Filter rule, supports syntax similar to the where clause in SQL                   | optional          |
| primary-keys              | Sink table primary keys, separated by commas                                      | optional          |
| partition-keys            | Sink table partition keys, separated by commas                                    | optional          |
| table-options             | used to the configure table creation statement when automatically creating tables | optional          |
| converter-after-transform | used to add a converter to change DataChangeEvent after transform                 | optional          |
| description               | Transform rule description                                                        | optional          |

多个transform规则可以声明在一个pipeline YAML文件中。

## converter-after-transform

`converter-after-transform` 用于在其他 transform 处理之后再修改 DataChangeEvent。可用的值如下：

- SOFT_DELETE: 删除事件会被转换成插入事件。这个转换器应该与元数据 `__data_event_type__` 一起使用，然后你可以实现软删除。

举个例子，下面的 transform 将不会删除数据，而是更新列 `op_type` 为 -D，然后转换成插入记录。

```yaml
transform:
  - source-table: \.*.\.*
    projection: \*, __data_event_type__ AS op_type
    converter-after-transform: SOFT_DELETE
```

# 元数据字段
## 字段定义
有一些隐藏列可以用来访问元数据信息。它们仅在显式引用在 transform 规则中时才会生效。

| Field               | Data Type | Description                                  |
|---------------------|-----------|----------------------------------------------|
| __namespace_name__  | String    | Name of the namespace that contains the row. |
| __schema_name__     | String    | Name of the schema that contains the row.    |
| __table_name__      | String    | Name of the table that contains the row.     |
| __data_event_type__ | String    | Operation type of data change event.         |

除了以上的元数据字段，连接器也可以解析额外的元数据并放入 DataChangeEvent 的 meta map中，这些元数据也可以在transform使用。
例如如下的作业，MySQL 连接器可以解析 `op_ts` 元数据并在transform中使用。

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: testuser
  password: testpwd
  tables: testdb.customer
  server-id: 5400-5404
  server-time-zone: UTC
  metadata.list: op_ts
  
transform:
  - source-table: testdb.customer
    projection: \*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, __data_event_type__ AS type, op_ts AS opts
  
sink:
  type: values
```

## Metadata relationship

| Type                 | Namespace | SchemaName | Table |
|----------------------|-----------|------------|-------|
| JDBC                 | Catalog   | Schema     | Table |
| Debezium             | Catalog   | Schema     | Table |
| MySQL                | Database  | -          | Table |
| Postgres             | Database  | Schema     | Table |
| Oracle               | -         | Schema     | Table |
| Microsoft SQL Server | Database  | Schema     | Table |
| StarRocks            | Database  | -          | Table |
| Doris                | Database  | -          | Table |

# 函数
Flink CDC 使用 [Calcite](https://calcite.apache.org/) 来解析表达式并且使用 [Janino script](https://www.janino.net/) 来执行表达式。

## 比较函数

| Function                             | Janino Code                                  | Description                                                                                           |
|--------------------------------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------|
| value1 = value2                      | valueEquals(value1, value2)                  | Returns TRUE if value1 is equal to value2; returns FALSE if value1 or value2 is NULL.                 |
| value1 <> value2                     | !valueEquals(value1, value2)                 | Returns TRUE if value1 is not equal to value2; returns FALSE if value1 or value2 is NULL.             |
| value1 > value2                      | greaterThan(value1, value2)                  | Returns TRUE if value1 is greater than value2; returns FALSE if value1 or value2 is NULL.             |
| value1 >= value2                     | greaterThanOrEqual(value1, value2)           | Returns TRUE if value1 is greater than or equal to value2; returns FALSE if value1 or value2 is NULL. |
| value1 < value2                      | lessThan(value1, value2)                     | Returns TRUE if value1 is less than value2; returns FALSE if value1 or value2 is NULL.                |
| value1 <= value2                     | lessThanOrEqual(value1, value2)              | Returns TRUE if value1 is less than or equal to value2; returns FALSE if value1 or value2 is NULL.    |
| value IS NULL                        | null == value                                | Returns TRUE if value is NULL.                                                                        |
| value IS NOT NULL                    | null != value                                | Returns TRUE if value is not NULL.                                                                    |
| value1 BETWEEN value2 AND value3     | betweenAsymmetric(value1, value2, value3)    | Returns TRUE if value1 is greater than or equal to value2 and less than or equal to value3.           |
| value1 NOT BETWEEN value2 AND value3 | notBetweenAsymmetric(value1, value2, value3) | Returns TRUE if value1 is less than value2 or greater than value3.                                    |
| string1 LIKE string2                 | like(string1, string2)                       | Returns TRUE if string1 matches pattern string2.                                                      |
| string1 NOT LIKE string2             | notLike(string1, string2)                    | Returns TRUE if string1 does not match pattern string2.                                               |
| value1 IN (value2 [, value3]* )      | in(value1, value2 [, value3]*)               | Returns TRUE if value1 exists in the given list (value2, value3, …).                                  |
| value1 NOT IN (value2 [, value3]* )  | notIn(value1, value2 [, value3]*)            | Returns TRUE if value1 does not exist in the given list (value2, value3, …).                          |

## 逻辑函数

| Function              | Janino Code                    | Description                                                         |
|-----------------------|--------------------------------|---------------------------------------------------------------------|
| boolean1 OR boolean2  | boolean1 &#124;&#124; boolean2 | Returns TRUE if BOOLEAN1 is TRUE or BOOLEAN2 is TRUE.               |
| boolean1 AND boolean2 | boolean1 && boolean2           | Returns TRUE if BOOLEAN1 and BOOLEAN2 are both TRUE.                |
| NOT boolean           | !boolean                       | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |
| boolean IS FALSE      | false == boolean               | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |
| boolean IS NOT FALSE  | true == boolean                | Returns TRUE if BOOLEAN is TRUE; returns FALSE if BOOLEAN is FALSE. |
| boolean IS TRUE       | true == boolean                | Returns TRUE if BOOLEAN is TRUE; returns FALSE if BOOLEAN is FALSE. |
| boolean IS NOT TRUE   | false == boolean               | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |

## 数学函数

| Function            | Janino Code         | Description                                                                                                                                                          |
|---------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| numeric1 + numeric2 | numeric1 + numeric2 | Returns NUMERIC1 plus NUMERIC2.                                                                                                                                      |
| numeric1 - numeric2 | numeric1 - numeric2 | Returns NUMERIC1 minus NUMERIC2.                                                                                                                                     |
| numeric1 * numeric2 | numeric1 * numeric2 | Returns NUMERIC1 multiplied by NUMERIC2.                                                                                                                             |
| numeric1 / numeric2 | numeric1 / numeric2 | Returns NUMERIC1 divided by NUMERIC2.                                                                                                                                |
| numeric1 % numeric2 | numeric1 % numeric2 | Returns the remainder (modulus) of numeric1 divided by numeric2.                                                                                                     |
| ABS(numeric)        | abs(numeric)        | Returns the absolute value of numeric.                                                                                                                               |
| CEIL(numeric)<br/>CEILING(numeric)       | ceil(numeric)       | Rounds numeric up, and returns the smallest number that is greater than or equal to numeric.                                                                         |
| FLOOR(numeric)      | floor(numeric)      | Rounds numeric down, and returns the largest number that is less than or equal to numeric.                                                                           |
| ROUND(numeric, int) | round(numeric)      | Returns a number rounded to INT decimal places for NUMERIC.                                                                                                          |
| UUID()              | uuid()              | Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. |

## 字符串函数

| Function                                         | Janino Code                              | Description                                                                                                                                                                                       |
|--------------------------------------------------|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string1 &#124;&#124; string2                     | concat(string1, string2)                 | Returns the concatenation of STRING1 and STRING2.                                                                                                                                                 |
| CHAR_LENGTH(string)                              | charLength(string)                       | Returns the number of characters in STRING.                                                                                                                                                       |
| UPPER(string)                                    | upper(string)                            | Returns string in uppercase.                                                                                                                                                                      |
| LOWER(string)                                    | lower(string)                            | Returns string in lowercase.                                                                                                                                                                      |
| TRIM(string1)                                    | trim('BOTH',string1)                     | Returns a string that removes whitespaces at both sides.                                                                                                                                          |
| REGEXP_REPLACE(string1, string2, string3)        | regexpReplace(string1, string2, string3) | Returns a string from STRING1 with all the substrings that match a regular expression STRING2 consecutively being replaced with STRING3. E.g., 'foobar'.regexpReplace('oo\|ar', '') returns "fb". |
| SUBSTR(string, integer1[, integer2])             | substr(string,integer1,integer2)         | Returns a substring of STRING starting from position integer1 with length integer2 (to the end by default).                                                                                       |
| SUBSTRING(string FROM integer1 [ FOR integer2 ]) | substring(string,integer1,integer2)      | Returns a substring of STRING starting from position integer1 with length integer2 (to the end by default).                                                                                       |
| CONCAT(string1, string2,…)                       | concat(string1, string2,…)               | Returns a string that concatenates string1, string2, …. E.g., CONCAT('AA', 'BB', 'CC') returns 'AABBCC'.                                                                                          |

## 时间函数

| Function                                             | Janino Code                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|------------------------------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| LOCALTIME                                            | localtime()                                          | Returns the current SQL time in the local time zone, the return type is TIME(0).                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| LOCALTIMESTAMP                                       | localtimestamp()                                     | Returns the current SQL timestamp in local time zone, the return type is TIMESTAMP(3).                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| CURRENT_TIME                                         | currentTime()                                        | Returns the current SQL time in the local time zone, this is a synonym of LOCAL_TIME.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| CURRENT_DATE                                         | currentDate()                                        | Returns the current SQL date in the local time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| CURRENT_TIMESTAMP                                    | currentTimestamp()                                   | Returns the current SQL timestamp in the local time zone, the return type is TIMESTAMP_LTZ(3).                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| NOW()                                                | now()                                                | Returns the current SQL timestamp in the local time zone, this is a synonym of CURRENT_TIMESTAMP.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| DATE_FORMAT(timestamp, string)                       | dateFormat(timestamp, string)                        | Converts timestamp to a value of string in the format specified by the date format string. The format string is compatible with Java's SimpleDateFormat.                                                                                                                                                                                                                                                                                                                                                                                            |
| TIMESTAMPADD(timeintervalunit, interval, timepoint)  | timestampadd(timeintervalunit, interval, timepoint)  | Returns the timestamp of timepoint2 after timepoint added interval. The unit for the interval is given by the first argument, which should be one of the following values: SECOND, MINUTE, HOUR, DAY, MONTH, or YEAR.                                                                                                                                                                                                                                                                                                                               |
| TIMESTAMPDIFF(timepointunit, timepoint1, timepoint2) | timestampDiff(timepointunit, timepoint1, timepoint2) | Returns the (signed) number of timepointunit between timepoint1 and timepoint2. The unit for the interval is given by the first argument, which should be one of the following values: SECOND, MINUTE, HOUR, DAY, MONTH, or YEAR.                                                                                                                                                                                                                                                                                                                   |
| TO_DATE(string1[, string2])                          | toDate(string1[, string2])                           | Converts a date string string1 with format string2 (by default 'yyyy-MM-dd') to a date.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| TO_TIMESTAMP(string1[, string2])                     | toTimestamp(string1[, string2])                      | Converts date time string string1 with format string2 (by default: 'yyyy-MM-dd HH:mm:ss') to a timestamp, without time zone.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| FROM_UNIXTIME(numeric[, string])                     | fromUnixtime(NUMERIC[, STRING])                      | Returns a representation of the numeric argument as a value in string format (default is ‘yyyy-MM-dd HH:mm:ss’). numeric is an internal timestamp value representing seconds since ‘1970-01-01 00:00:00’ UTC, such as produced by the UNIX_TIMESTAMP() function. The return value is expressed in the session time zone (specified in TableConfig). E.g., FROM_UNIXTIME(44) returns ‘1970-01-01 00:00:44’ if in UTC time zone, but returns ‘1970-01-01 09:00:44’ if in ‘Asia/Tokyo’ time zone.                                                      |
| UNIX_TIMESTAMP()                                     | unixTimestamp()                                      | Gets current Unix timestamp in seconds. This function is not deterministic which means the value would be recalculated for each record.                                                                                                                                                                                                                                                                                                                                                                                                             |
| UNIX_TIMESTAMP(string1[, string2])                   | unixTimestamp(STRING1[, STRING2])                    | Converts a date time string string1 with format string2 (by default: yyyy-MM-dd HH:mm:ss if not specified) to Unix timestamp (in seconds), using the specified timezone in table config.<br/>If a time zone is specified in the date time string and parsed by UTC+X format such as “yyyy-MM-dd HH:mm:ss.SSS X”, this function will use the specified timezone in the date time string instead of the timezone in table config. If the date time string can not be parsed, the default value Long.MIN_VALUE(-9223372036854775808) will be returned. |

## 条件函数

| Function                                                                                                              | Janino Code                          | Description                                                                                                                                                                                                                                       |
|-----------------------------------------------------------------------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASE value WHEN value1_1 [, value1_2]* THEN RESULT1 (WHEN value2_1 [, value2_2 ]* THEN result_2)* (ELSE result_z) END | Nested ternary expression            | Returns resultX when the first time value is contained in (valueX_1, valueX_2, …). When no value matches, returns result_z if it is provided and returns NULL otherwise.                                                                          |
| CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END                                 | Nested ternary expression            | Returns resultX when the first conditionX is met. When no condition is met, returns result_z if it is provided and returns NULL otherwise.                                                                                                        |
| COALESCE(value1 [, value2]*)                                                                                          | coalesce(Object... objects)          | Returns the first argument that is not NULL.If all arguments are NULL, it returns NULL as well. The return type is the least restrictive, common type of all of its arguments. The return type is nullable if all arguments are nullable as well. |
| IF(condition, true_value, false_value)                                                                                | condition ? true_value : false_value | Returns the true_value if condition is met, otherwise false_value. E.g., IF(5 > 3, 5, 3) returns 5.                                                                                                                                               |

## 转换函数

You can use `CAST( <EXPR> AS <T> )` syntax to convert any valid expression `<EXPR>` to a specific type `<T>`. Possible conversion paths are:

| Source Type                         | Target Type | Notes                                                                                      |
|-------------------------------------|-------------|--------------------------------------------------------------------------------------------|
| ANY                                 | STRING      | All types can be cast to STRING.                                                           |
| NUMERIC, STRING                     | BOOLEAN     | Any non-zero numerics will be evaluated to `TRUE`.                                         |
| NUMERIC                             | BYTE        | Value must be in the range of Byte (-128 ~ 127).                                           |
| NUMERIC                             | SHORT       | Value must be in the range of Short (-32768 ~ 32767).                                      |
| NUMERIC                             | INTEGER     | Value must be in the range of Integer (-2147483648 ~ 2147483647).                          |
| NUMERIC                             | LONG        | Value must be in the range of Long (-9223372036854775808 ~ 9223372036854775807).           |
| NUMERIC                             | FLOAT       | Value must be in the range of Float (1.40239846e-45f ~ 3.40282347e+38f).                   |
| NUMERIC                             | DOUBLE      | Value must be in the range of Double (4.94065645841246544e-324 ~ 1.79769313486231570e+308) |
| NUMERIC                             | DECIMAL     | Value must be in the range of BigDecimal(10, 0).                                           |
| STRING, TIMESTAMP_TZ, TIMESTAMP_LTZ | TIMESTAMP   | String type value must be a valid `ISO_LOCAL_DATE_TIME` string.                            |

# 示例
## 添加计算列
表达式可以用来生成新的列。例如，如果我们想基于表 `web_order` 在数据库 `mydb` 中添加两个计算列，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name, localtimestamp as new_timestamp
    description: append calculated columns based on source table
```

## 引用元数据列
我们可以在投影表达式中引用元数据列。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：
```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name
    description: access metadata columns from source table
```

## 使用通配符来包含所有字段
通配符(`*`)可以用来引用表中的所有字段。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: \*, UPPER(product_name) as product_name
    description: project fields with wildcard character from source table
  - source-table: mydb.app_order
    projection: UPPER(product_name) as product_name, *
    description: project fields with wildcard character from source table
```
注意：当 `*` 字符出现在表达式的开头时，需要添加转义反斜杠。

## 添加过滤条件
使用引用列时，我们可以在添加过滤规则时使用。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    filter: id > 10 AND order_id > 100
    description: filtering rows from source table
```

计算列可以在过滤条件中使用。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(province) as new_province 
    filter: new_province = 'SHANGHAI'
    description: filtering rows based on computed columns
```

## 重新设置主键
我们可以在转换规则中重新设置主键。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id
    primary-keys: order_id
    description: reassign primary key example
```

复合主键也支持。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    primary-keys: order_id, product_name
    description: reassign composite primary keys example
```

注意主键列不能为空。

## 重新设置分区键
我们可以重新设置分区键。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：
```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    partition-keys: product_name
    description: reassign partition key example
```

## 定义建表参数
额外的建表参数可以定义在转换规则中，并在创建下游表时应用。例如，给定表 `web_order` 在数据库 `mydb` 中，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    table-options: comment=web order
    description: auto creating table options example
```
小技巧：table-options 的格式是 `key1=value1,key2=value2`。

## Classification mapping
多个转换规则可以定义为分类映射。
只有第一个匹配的转换规则将应用。
举个例子，我们可以定义一个转换规则如下：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id
    filter: UPPER(province) = 'SHANGHAI'
    description: classification mapping example
  - source-table: mydb.web_order
    projection: order_id as id, id as order_id
    filter: UPPER(province) = 'BEIJING'
    description: classification mapping example
```

## 用户自定义函数
用户自定义函数（UDF）可以在转换规则中使用。

一个满足了下面条件的类可以作为 UDF:
* 继承了 `org.apache.flink.cdc.common.udf.UserDefinedFunction` 接口
* 有一个 public 的无参构造函数
* 有至少一个 public 方法命名为 `eval`

也可以:

* 重载 `getReturnType` 方法来指示它的返回 CDC 类型
* 重载 `open` 和 `close` 方法来执行一些初始化和清理工作

举个例子，这是一个有效的 UDF 类:

```java
public class AddOneFunctionClass implements UserDefinedFunction {
    
    public Object eval(Integer num) {
        return num + 1;
    }
    
    @Override
    public DataType getReturnType() {
        return DataTypes.INT();
    }
    
    @Override
    public void open() throws Exception {
        // ...
    }

    @Override
    public void close() throws Exception {
        // ...
    }
}
```

为了兼容 Flink SQL，UDF 类也可以是 Flink `ScalarFunction`，但有一些限制:

* 包含有参构造方法的 `ScalarFunction` 是不支持的。
* FLink 的 `DataType` 提示在 `ScalarFunction` 中将被忽略。
* `open` / `close` 生命周期钩子不会被调用。

UDF 类可以通过 `user-defined-function` 块进行注册：

```yaml
pipeline:
  user-defined-function:
    - name: addone
      classpath: org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass
    - name: format
      classpath: org.apache.flink.cdc.udf.examples.java.FormatFunctionClass
```

注意这里的 `classpath` 必须是全限定名，并且对应的 `jar` 文件必须包含在 Flink `/lib` 文件夹中，或者通过 `flink-cdc.sh --jar` 选项传递。

在正确注册后，UDF 可以在 `projection` 和 `filter` 表达式中使用，就像内置函数一样：

```yaml
transform:
  - source-table: db.\.*
    projection: "*, inc(inc(inc(id))) as inc_id, format(id, 'id -> %d') as formatted_id"
    filter: inc(id) < 100
```

## 内置 AI 模型

内置AI模型可以在transform规则中使用。
为了使用内置AI模型，你需要下载内置模型的jar，然后在flink-cdc.sh命令中添加`--jar {$BUILT_IN_MODEL_PATH}`。

如何定义一个Embedding AI模型：

```yaml
pipeline:
  model:
    - model-name: CHAT
      class-name: OpenAIChatModel
      openai.model: gpt-4o-mini
      openai.host: https://xxxx
      openai.apikey: abcd1234
      openai.chat.prompt: please summary this
    - model-name: GET_EMBEDDING
      class-name: OpenAIEmbeddingModel
      openai.model: text-embedding-3-small
      openai.host: https://xxxx
      openai.apikey: abcd1234
```
注意：
* `model-name` 是一个通用的必填参数，用于所有支持的模型，表示在`projection`或`filter`中调用的函数名称。
* `class-name` 是一个通用的必填参数，用于所有支持的模型，可用值可以在[所有支持的模型](#all-support-models)中找到。
* `openai.model`，`openai.host`， `openai.apiKey` 和 `openai.chat.prompt` 是在各个模型中特别的可选参数。

如何使用一个内置的 AI 模型：

```yaml
transform:
  - source-table: db.\.*
    projection: "*, inc(inc(inc(id))) as inc_id, GET_EMBEDDING(page) as emb, CHAT(page) as summary"
    filter: inc(id) < 100
pipeline:
  model:
    - model-name: CHAT
      class-name: OpenAIChatModel
      openai.model: gpt-4o-mini
      openai.host: http://langchain4j.dev/demo/openai/v1
      openai.apikey: demo
      openai.chat.prompt: please summary this
    - model-name: GET_EMBEDDING
      class-name: OpenAIEmbeddingModel
      openai.model: text-embedding-3-small
      openai.host: http://langchain4j.dev/demo/openai/v1
      openai.apikey: demo
```
这里，GET_EMBEDDING 是通过`model-name`在`pipeline`中定义的。

### 所有支持的模型

下面列出了所有支持的模型：

#### OpenAIChatModel

| 参数                 | 类型     | optional/required | 含义                                                                                                                                   |
|--------------------|--------|-------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| openai.model       | STRING | required          | Name of model to be called, for example: "gpt-4o-mini", Available options are "gpt-4o-mini", "gpt-4o", "gpt-4-32k", "gpt-3.5-turbo". |
| openai.host        | STRING | required          | Host of the Model server to be connected, for example: `http://langchain4j.dev/demo/openai/v1`.                                      |
| openai.apikey      | STRING | required          | Api Key for verification of the Model server, for example, "demo".                                                                   |
| openai.chat.prompt | STRING | optional          | Prompt for chatting with OpenAI, for example: "Please summary this ".                                                                |

#### OpenAIEmbeddingModel

| 参数            | 类型     | optional/required | 含义                                                                                                                                                                     |
|---------------|--------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| openai.model  | STRING | required          | Name of model to be called, for example: "text-embedding-3-small", Available options are "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002". |
| openai.host   | STRING | required          | Host of the Model server to be connected, for example: `http://langchain4j.dev/demo/openai/v1`.                                                                        |
| openai.apikey | STRING | required          | Api Key for verification of the Model server, for example, "demo".                                                                                                     |