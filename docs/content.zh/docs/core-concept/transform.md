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
为了定义一个 transform 规则，可以使用以下参数：

| 参数                        | 含义                                                     | 是否必填     |
|---------------------------|--------------------------------------------------------|----------|
| source-table              | 源表 ID，支持正则表达式                                         | 必填       |
| projection                | 投影规则，支持类似 SQL 中 SELECT 子句的语法                           | 可选       |
| filter                    | 过滤规则，支持类似 SQL 中 WHERE 子句的语法                            | 可选       |
| primary-keys              | 目标表主键，以逗号分隔                                            | 可选       |
| partition-keys            | 目标表分区键，以逗号分隔                                           | 可选       |
| table-options             | 用于配置自动建表时的建表语句                                         | 可选       |
| converter-after-transform | 用于在 transform 处理后添加转换器来修改 DataChangeEvent                | 可选       |
| description               | Transform 规则描述                                         | 可选       |

多个 transform 规则可以声明在一个 pipeline YAML 文件中。

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
有一些隐藏列可以用来访问元数据信息。它们仅在 transform 规则中显式引用时才会生效。

| 字段                  | 数据类型   | 描述                  |
|---------------------|--------|---------------------|
| __namespace_name__  | String | 包含该行数据的命名空间名称。      |
| __schema_name__     | String | 包含该行数据的 schema 名称。  |
| __table_name__      | String | 包含该行数据的表名称。         |
| __data_event_type__ | String | 数据变更事件的操作类型。        |

除了以上的元数据字段，连接器也可以解析额外的元数据并放入 DataChangeEvent 的 meta map 中，这些元数据也可以在 transform 中使用。
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

## 元数据关系

| 类型                   | Namespace | SchemaName | Table |
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

| 函数                                   | Janino 代码                                    | 描述                                                          |
|--------------------------------------|----------------------------------------------|-------------------------------------------------------------|
| value1 = value2                      | valueEquals(value1, value2)                  | 如果 value1 等于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。   |
| value1 <> value2                     | !valueEquals(value1, value2)                 | 如果 value1 不等于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。  |
| value1 > value2                      | greaterThan(value1, value2)                  | 如果 value1 大于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。   |
| value1 >= value2                     | greaterThanOrEqual(value1, value2)           | 如果 value1 大于等于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。 |
| value1 < value2                      | lessThan(value1, value2)                     | 如果 value1 小于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。   |
| value1 <= value2                     | lessThanOrEqual(value1, value2)              | 如果 value1 小于等于 value2，返回 TRUE；如果 value1 或 value2 为 NULL，返回 FALSE。 |
| value IS NULL                        | null == value                                | 如果 value 为 NULL，返回 TRUE。                                      |
| value IS NOT NULL                    | null != value                                | 如果 value 不为 NULL，返回 TRUE。                                     |
| value1 BETWEEN value2 AND value3     | betweenAsymmetric(value1, value2, value3)    | 如果 value1 大于等于 value2 且小于等于 value3，返回 TRUE。                    |
| value1 NOT BETWEEN value2 AND value3 | notBetweenAsymmetric(value1, value2, value3) | 如果 value1 小于 value2 或大于 value3，返回 TRUE。                        |
| string1 LIKE string2                 | like(string1, string2)                       | 如果 string1 匹配模式 string2，返回 TRUE。                              |
| string1 NOT LIKE string2             | notLike(string1, string2)                    | 如果 string1 不匹配模式 string2，返回 TRUE。                             |
| value1 IN (value2 [, value3]* )      | in(value1, value2 [, value3]*)               | 如果 value1 存在于给定列表 (value2, value3, …) 中，返回 TRUE。              |
| value1 NOT IN (value2 [, value3]* )  | notIn(value1, value2 [, value3]*)            | 如果 value1 不存在于给定列表 (value2, value3, …) 中，返回 TRUE。             |

## 逻辑函数

| 函数                    | Janino 代码                      | 描述                                        |
|-----------------------|--------------------------------|-------------------------------------------|
| boolean1 OR boolean2  | boolean1 &#124;&#124; boolean2 | 如果 BOOLEAN1 为 TRUE 或 BOOLEAN2 为 TRUE，返回 TRUE。 |
| boolean1 AND boolean2 | boolean1 && boolean2           | 如果 BOOLEAN1 和 BOOLEAN2 都为 TRUE，返回 TRUE。      |
| NOT boolean           | !boolean                       | 如果 boolean 为 FALSE，返回 TRUE；如果 boolean 为 TRUE，返回 FALSE。 |
| boolean IS FALSE      | false == boolean               | 如果 boolean 为 FALSE，返回 TRUE；如果 boolean 为 TRUE，返回 FALSE。 |
| boolean IS NOT FALSE  | true == boolean                | 如果 BOOLEAN 为 TRUE，返回 TRUE；如果 BOOLEAN 为 FALSE，返回 FALSE。 |
| boolean IS TRUE       | true == boolean                | 如果 BOOLEAN 为 TRUE，返回 TRUE；如果 BOOLEAN 为 FALSE，返回 FALSE。 |
| boolean IS NOT TRUE   | false == boolean               | 如果 boolean 为 FALSE，返回 TRUE；如果 boolean 为 TRUE，返回 FALSE。 |

## 数学函数

| 函数                                 | Janino 代码           | 描述                                                                                          |
|------------------------------------|---------------------|---------------------------------------------------------------------------------------------|
| numeric1 + numeric2                | numeric1 + numeric2 | 返回 NUMERIC1 加 NUMERIC2 的结果。                                                                  |
| numeric1 - numeric2                | numeric1 - numeric2 | 返回 NUMERIC1 减 NUMERIC2 的结果。                                                                  |
| numeric1 * numeric2                | numeric1 * numeric2 | 返回 NUMERIC1 乘 NUMERIC2 的结果。                                                                  |
| numeric1 / numeric2                | numeric1 / numeric2 | 返回 NUMERIC1 除以 NUMERIC2 的结果。                                                                 |
| numeric1 % numeric2                | numeric1 % numeric2 | 返回 numeric1 除以 numeric2 的余数（取模）。                                                             |
| ABS(numeric)                       | abs(numeric)        | 返回 numeric 的绝对值。                                                                            |
| CEIL(numeric)<br/>CEILING(numeric) | ceil(numeric)       | 向上取整，返回大于或等于 numeric 的最小整数。                                                                  |
| FLOOR(numeric)                     | floor(numeric)      | 向下取整，返回小于或等于 numeric 的最大整数。                                                                  |
| ROUND(numeric, int)                | round(numeric)      | 返回 NUMERIC 四舍五入到 INT 位小数的数字。                                                                 |
| UUID()                             | uuid()              | 根据 RFC 4122 类型 4（伪随机生成）返回一个 UUID（通用唯一标识符）字符串（例如 "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e"）。 |

## 字符串函数

| 函数                                               | Janino 代码                                | 描述                                                                                                             |
|--------------------------------------------------|------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| string1 &#124;&#124; string2                     | concat(string1, string2)                 | 返回 STRING1 和 STRING2 连接后的字符串。                                                                                  |
| CHAR_LENGTH(string)                              | charLength(string)                       | 返回 STRING 中的字符数。                                                                                               |
| UPPER(string)                                    | upper(string)                            | 返回大写形式的字符串。                                                                                                    |
| LOWER(string)                                    | lower(string)                            | 返回小写形式的字符串。                                                                                                    |
| TRIM(string1)                                    | trim('BOTH',string1)                     | 返回去除两端空格的字符串。                                                                                                  |
| REGEXP_REPLACE(string1, string2, string3)        | regexpReplace(string1, string2, string3) | 返回将 STRING1 中所有匹配正则表达式 STRING2 的子串替换为 STRING3 后的字符串。例如，'foobar'.regexpReplace('oo\|ar', '') 返回 "fb"。         |
| SUBSTR(string, integer1[, integer2])             | substr(string,integer1,integer2)         | 返回 STRING 从位置 integer1 开始、长度为 integer2（默认到末尾）的子串。                                                              |
| SUBSTRING(string FROM integer1 [ FOR integer2 ]) | substring(string,integer1,integer2)      | 返回 STRING 从位置 integer1 开始、长度为 integer2（默认到末尾）的子串。                                                              |
| CONCAT(string1, string2,…)                       | concat(string1, string2,…)               | 返回连接 string1、string2、… 后的字符串。例如，CONCAT('AA', 'BB', 'CC') 返回 'AABBCC'。                                          |

## 时间函数

| 函数                                                   | Janino 代码                                            | 描述                                                                                                                                                                                                                                                                                               |
|------------------------------------------------------|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| LOCALTIME                                            | localtime()                                          | 返回本地时区的当前 SQL 时间，返回类型为 TIME(0)。                                                                                                                                                                                                                                                                  |
| LOCALTIMESTAMP                                       | localtimestamp()                                     | 返回本地时区的当前 SQL 时间戳，返回类型为 TIMESTAMP(3)。                                                                                                                                                                                                                                                            |
| CURRENT_TIME                                         | currentTime()                                        | 返回本地时区的当前 SQL 时间，是 LOCAL_TIME 的同义词。                                                                                                                                                                                                                                                              |
| CURRENT_DATE                                         | currentDate()                                        | 返回本地时区的当前 SQL 日期。                                                                                                                                                                                                                                                                                |
| CURRENT_TIMESTAMP                                    | currentTimestamp()                                   | 返回本地时区的当前 SQL 时间戳，返回类型为 TIMESTAMP_LTZ(3)。                                                                                                                                                                                                                                                        |
| NOW()                                                | now()                                                | 返回本地时区的当前 SQL 时间戳，是 CURRENT_TIMESTAMP 的同义词。                                                                                                                                                                                                                                                      |
| DATE_FORMAT(timestamp, string)                       | dateFormat(timestamp, string)                        | 将时间戳转换为指定日期格式字符串的值。格式字符串与 Java 的 SimpleDateFormat 兼容。                                                                                                                                                                                                                                            |
| DATE_FORMAT(date, string)                            | dateFormat(date, string)                             | 将给定日期转换为指定格式字符串的值。格式字符串与 Java 的 SimpleDateFormat 兼容。                                                                                                                                                                                                                                             |
| DATE_FORMAT(time, string)                            | dateFormat(time, string)                             | 将给定时间转换为指定格式字符串的值。格式字符串与 Java 的 SimpleDateFormat 兼容。                                                                                                                                                                                                                                             |
| DATE_FORMAT_TZ(timestamp, format, timezone)          | dateFormatTz(timestamp, format, timezone)            | 使用给定的模式和指定的时区将时间戳或日期时间值格式化为字符串。timezone 参数可以是时区 ID（例如 'UTC'、'Asia/Shanghai'）或偏移量（如 '+08:00'）。                                                                                                                                                                                                       |
| TIMESTAMPADD(timeintervalunit, interval, timepoint)  | timestampadd(timeintervalunit, interval, timepoint)  | 返回 timepoint 加上 interval 后的时间戳。时间间隔的单位由第一个参数指定，应为以下值之一：SECOND、MINUTE、HOUR、DAY、MONTH 或 YEAR。                                                                                                                                                                                                     |
| TIMESTAMPDIFF(timepointunit, timepoint1, timepoint2) | timestampDiff(timepointunit, timepoint1, timepoint2) | 返回 timepoint1 和 timepoint2 之间的（有符号）时间单位数。时间间隔的单位由第一个参数指定，应为以下值之一：SECOND、MINUTE、HOUR、DAY、MONTH 或 YEAR。                                                                                                                                                                                             |
| TO_DATE(string1[, string2])                          | toDate(string1[, string2])                           | 将日期字符串 string1 按格式 string2（默认为 'yyyy-MM-dd'）转换为日期。                                                                                                                                                                                                                                               |
| TO_TIMESTAMP(string1[, string2])                     | toTimestamp(string1[, string2])                      | 将日期时间字符串 string1 按格式 string2（默认为 'yyyy-MM-dd HH:mm:ss'）转换为时间戳，不带时区。                                                                                                                                                                                                                              |
| TO_TIMESTAMP_LTZ(string1[, string2])                 | toTimestampLtz(string1[, string2])                   | 将日期时间字符串 string1 按格式 string2（默认为 'yyyy-MM-dd HH:mm:ss'）转换为时间戳，带本地时区。                                                                                                                                                                                                                             |
| FROM_UNIXTIME(numeric[, string])                     | fromUnixtime(NUMERIC[, STRING])                      | 返回 numeric 参数的字符串格式表示（默认为 'yyyy-MM-dd HH:mm:ss'）。numeric 是表示自 '1970-01-01 00:00:00' UTC 以来秒数的内部时间戳值，例如由 UNIX_TIMESTAMP() 函数产生的值。返回值以会话时区（在 TableConfig 中指定）表示。例如，如果在 UTC 时区，FROM_UNIXTIME(44) 返回 '1970-01-01 00:00:44'，但如果在 'Asia/Tokyo' 时区则返回 '1970-01-01 09:00:44'。                                   |
| UNIX_TIMESTAMP()                                     | unixTimestamp()                                      | 获取当前 Unix 时间戳（秒）。此函数是非确定性的，意味着每条记录都会重新计算该值。                                                                                                                                                                                                                                                      |
| UNIX_TIMESTAMP(string1[, string2])                   | unixTimestamp(STRING1[, STRING2])                    | 将日期时间字符串 string1 按格式 string2（如果未指定，默认为 yyyy-MM-dd HH:mm:ss）转换为 Unix 时间戳（秒），使用表配置中指定的时区。<br/>如果日期时间字符串中指定了时区并以 UTC+X 格式（如 "yyyy-MM-dd HH:mm:ss.SSS X"）解析，则此函数将使用日期时间字符串中指定的时区而不是表配置中的时区。如果无法解析日期时间字符串，将返回默认值 Long.MIN_VALUE(-9223372036854775808)。 |
| DATE_ADD(date, int)                                  | dateAdd(date, int)                                   | 将 N 天添加到给定日期，返回格式为 'yyyy-MM-dd' 的字符串。                                                                                                                                                                                                                                                            |

## 条件函数

| 函数                                                                                                                    | Janino 代码                        | 描述                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------|----------------------------------|------------------------------------------------------------------------------------------------------------|
| CASE value WHEN value1_1 [, value1_2]* THEN RESULT1 (WHEN value2_1 [, value2_2 ]* THEN result_2)* (ELSE result_z) END | 嵌套三元表达式                          | 当 value 第一次匹配到 (valueX_1, valueX_2, …) 中的值时，返回 resultX。如果没有匹配的值，如果提供了 result_z 则返回 result_z，否则返回 NULL。     |
| CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END                                 | 嵌套三元表达式                          | 当第一个 conditionX 满足时，返回 resultX。如果没有条件满足，如果提供了 result_z 则返回 result_z，否则返回 NULL。                              |
| COALESCE(value1 [, value2]*)                                                                                          | coalesce(Object... objects)      | 返回第一个不为 NULL 的参数。如果所有参数都为 NULL，则返回 NULL。返回类型是所有参数中限制最少的公共类型。如果所有参数都可为空，则返回类型也可为空。                          |
| IF(condition, true_value, false_value)                                                                                | condition ? true_value : false_value | 如果条件满足，返回 true_value，否则返回 false_value。例如，IF(5 > 3, 5, 3) 返回 5。                                              |

## 转换函数

你可以使用 `CAST( <EXPR> AS <T> )` 语法将任何有效的表达式 `<EXPR>` 转换为特定类型 `<T>`。可能的转换路径如下：

| 源类型                                 | 目标类型      | 说明                                               |
|-------------------------------------|-----------|--------------------------------------------------|
| ANY                                 | STRING    | 所有类型都可以转换为 STRING。                               |
| NUMERIC, STRING                     | BOOLEAN   | 任何非零数字将被计算为 `TRUE`。                              |
| NUMERIC                             | BYTE      | 值必须在 Byte 范围内 (-128 ~ 127)。                      |
| NUMERIC                             | SHORT     | 值必须在 Short 范围内 (-32768 ~ 32767)。                 |
| NUMERIC                             | INTEGER   | 值必须在 Integer 范围内 (-2147483648 ~ 2147483647)。     |
| NUMERIC                             | LONG      | 值必须在 Long 范围内 (-9223372036854775808 ~ 9223372036854775807)。 |
| NUMERIC                             | FLOAT     | 值必须在 Float 范围内 (1.40239846e-45f ~ 3.40282347e+38f)。 |
| NUMERIC                             | DOUBLE    | 值必须在 Double 范围内 (4.94065645841246544e-324 ~ 1.79769313486231570e+308)。 |
| NUMERIC                             | DECIMAL   | 值必须在 BigDecimal(10, 0) 范围内。                      |
| STRING, TIMESTAMP_TZ, TIMESTAMP_LTZ | TIMESTAMP | 字符串类型的值必须是有效的 `ISO_LOCAL_DATE_TIME` 格式字符串。       |

## Variant 函数

| 函数 | Janino 代码 | 描述 |
| -------- | ----------- | ----------- |
| PARSE_JSON(string[, allowDuplicateKeys]) | parseJson(string[, allowDuplicateKeys]) | 将 JSON 字符串解析为 Variant。如果 JSON 字符串无效，将抛出错误。如需返回 NULL 而不是抛出错误，请使用 TRY_PARSE_JSON 函数。<br><br>如果输入的 JSON 字符串中存在重复的键，当 `allowDuplicateKeys` 为 true 时，解析器将保留所有相同键的最后一个出现的字段，否则将抛出错误。`allowDuplicateKeys` 的默认值为 false。 |
| TRY_PARSE_JSON(string[, allowDuplicateKeys]) | tryParseJson(string[, allowDuplicateKeys]) | 如果可能，将 JSON 字符串解析为 Variant。如果 JSON 字符串无效，返回 NULL。如需抛出错误而不是返回 NULL，请使用 PARSE_JSON 函数。<br><br>如果输入的 JSON 字符串中存在重复的键，当 `allowDuplicateKeys` 为 true 时，解析器将保留所有相同键的最后一个出现的字段，否则将返回 NULL。`allowDuplicateKeys` 的默认值为 false。 |

## 结构函数

结构函数用于使用下标语法访问 ARRAY、MAP、ROW 和 VARIANT 类型中的元素。

| 函数 | Janino 代码 | 描述 |
| -------- | ----------- | ----------- |
| array[index] | itemAccess(array, index) | 返回数组中位置 `index` 的元素。索引从 1 开始（SQL 标准）。如果索引超出范围或数组为 NULL，则返回 NULL。 |
| map[key] | itemAccess(map, key) | 返回 map 中与 `key` 关联的值。如果 key 不存在或 map 为 NULL，则返回 NULL。 |
| row[index] | itemAccess(row, index) | 返回 row 中位置 `index` 的字段。索引从 1 开始。索引必须是常量（不能是计算表达式），因为返回类型必须在静态阶段确定。 |
| variant[index] | itemAccess(variant, index) | 返回 variant 中位置 `index` 的元素（如果是数组）。索引从 1 开始（SQL 标准）。如果索引超出范围或 variant 不是数组，则返回 NULL。 |
| variant[key] | itemAccess(variant, key) | 返回 variant 中与 `key` 关联的值（如果是对象）。如果 key 不存在或 variant 不是对象，则返回 NULL。 |

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

## 分类映射
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
* Flink 的 `DataType` 提示在 `ScalarFunction` 中将被忽略。
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

## Embedding AI 模型

Embedding AI 模型可以在 transform 规则中使用。
为了使用 Embedding AI 模型，你需要下载内置模型的 jar，然后在 `flink-cdc.sh` 命令中添加 `--jar {$BUILT_IN_MODEL_PATH}`。

如何定义一个 Embedding AI 模型：

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
* `model-name` 是一个通用的必填参数，用于所有支持的模型，表示在 `projection` 或 `filter` 中调用的函数名称。
* `class-name` 是一个通用的必填参数，用于所有支持的模型，可用值可以在[所有支持的模型](#all-support-models)中找到。
* `openai.model`，`openai.host`，`openai.apiKey` 和 `openai.chat.prompt` 是在各个模型中特别的可选参数。

如何使用一个 Embedding AI 模型：

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
这里，GET_EMBEDDING 是通过 `model-name` 在 `pipeline` 中定义的。

### 所有支持的模型

下面列出了所有支持的模型：

#### OpenAIChatModel

| 参数                 | 类型     | 是否必填     | 含义                                                                                                 |
|--------------------|--------|----------|-----------------------------------------------------------------------------------------------------|
| openai.model       | STRING | 必填       | 要调用的模型名称，例如："gpt-4o-mini"，可用选项有 "gpt-4o-mini"、"gpt-4o"、"gpt-4-32k"、"gpt-3.5-turbo"。 |
| openai.host        | STRING | 必填       | 要连接的模型服务器地址，例如：`http://langchain4j.dev/demo/openai/v1`。                                 |
| openai.apikey      | STRING | 必填       | 模型服务器验证的 API Key，例如："demo"。                                                                |
| openai.chat.prompt | STRING | 可选       | 与 OpenAI 聊天的提示词，例如："Please summary this"。                                                   |

#### OpenAIEmbeddingModel

| 参数            | 类型     | 是否必填     | 含义                                                                                                                   |
|---------------|--------|----------|----------------------------------------------------------------------------------------------------------------------|
| openai.model  | STRING | 必填       | 要调用的模型名称，例如："text-embedding-3-small"，可用选项有 "text-embedding-3-small"、"text-embedding-3-large"、"text-embedding-ada-002"。 |
| openai.host   | STRING | 必填       | 要连接的模型服务器地址，例如：`http://langchain4j.dev/demo/openai/v1`。                                                   |
| openai.apikey | STRING | 必填       | 模型服务器验证的 API Key，例如："demo"。                                                                                  |