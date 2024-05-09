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

# Definition
**Transform** module helps users delete and expand data columns based on the data columns in the table.      
What's more, it also helps users filter some unnecessary data during the synchronization process.

# Parameters
To describe a transform, the follows are required:

| Parameter    | Meaning                                            | Optional/Required |
|--------------|----------------------------------------------------|-------------------|
| source-table | Source table id, supports regular expressions      | required          |
| projection   | Projection rule, supports syntax similar to the select clause in SQL        | optional          |
| filter       | Filter rule, supports syntax similar to the where clause in SQL        | optional          |
| primary-keys | Sink table primary keys, it is separated by English commas        | optional          |
| partition-keys | Sink table partition keys, it is separated by English commas        | optional          |
| table-options | Sink table options, when automatically creating a table, it is added to the configuration of the table creation statement  | optional          |
| description  | Transform rule description(a default value provided) | optional          |

A transform module can contain a list of source-table rules.

# Metadata Fields
## Fields definition
There are three hidden columns used to describe metadata information. They only take effect when explicitly referenced in the transform rules.

| Field               | Data Type | Description                                                     |
|--------------------|-----------|--------------------------------------------------------------|
| __namespace_name__ | String    | Name of the namespace that contain the row.                  |
| __schema_name__    | String    | Name of the schema that contain the row.                     |
| __table_name__     | String    | Name of the table that contain the row.                      |

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

# Functions
Using Calcite to implement function parsing, and using Janino script to implement function calls.

## Comparison Functions

| Function             | Janino Code                 | Description                                                     |
|----------------------|-----------------------------|-----------------------------------------------------------------|
| value1 = value2      | valueEquals(value1, value2) | Returns TRUE if value1 is equal to value2; returns FALSE if value1 or value2 is NULL. |
| value1 <> value2     | !valueEquals(value1, value2) | Returns TRUE if value1 is not equal to value2; returns FALSE if value1 or value2 is NULL. |
| value1 > value2      | value1 > value2             | Returns TRUE if value1 is greater than value2; returns FALSE if value1 or value2 is NULL. |
| value1 >= value2     | value1 >= value2            | Returns TRUE if value1 is greater than or equal to value2; returns FALSE if value1 or value2 is NULL. |
| value1 < value2      | value1 < value2             | Returns TRUE if value1 is less than value2; returns FALSE if value1 or value2 is NULL. |
| value1 <= value2     | value1 <= value2            | Returns TRUE if value1 is less than or equal to value2; returns FALSE if value1 or value2 is NULL. |
| value IS NULL        | null == value               | Returns TRUE if value is NULL.                                  |
| value IS NOT NULL    | null != value               | Returns TRUE if value is not NULL.                              |
| value1 BETWEEN value2 AND value3 | betweenAsymmetric(value1, value2, value3) | Returns TRUE if value1 is greater than or equal to value2 and less than or equal to value3. |
| value1 NOT BETWEEN value2 AND value3 | notBetweenAsymmetric(value1, value2, value3) | Returns TRUE if value1 is less than value2 or greater than value3. |
| string1 LIKE string2 | like(string1, string2)      | Returns TRUE if string1 matches pattern string2.                |
| string1 NOT LIKE string2 | notLike(string1, string2) | Returns TRUE if string1 does not match pattern string2.       |
| value1 IN (value2 [, value3]* ) | in(value1, value2 [, value3]*) | Returns TRUE if value1 exists in the given list (value2, value3, …). |
| value1 NOT IN (value2 [, value3]* ) | notIn(value1, value2 [, value3]*) | Returns TRUE if value1 does not exist in the given list (value2, value3, …).  |

## Logical Functions

| Function             | Janino Code                 | Description                                                     |
|----------------------|-----------------------------|-----------------------------------------------------------------|
| boolean1 OR boolean2 | boolean1 &#124;&#124; boolean2 | Returns TRUE if BOOLEAN1 is TRUE or BOOLEAN2 is TRUE.        |
| boolean1 AND boolean2 | boolean1 && boolean2       | Returns TRUE if BOOLEAN1 and BOOLEAN2 are both TRUE.            |
| NOT boolean          | !boolean                    | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |
| boolean IS FALSE     | false == boolean            | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |
| boolean IS NOT FALSE | true == boolean             | Returns TRUE if BOOLEAN is TRUE; returns FALSE if BOOLEAN is FALSE. |
| boolean IS TRUE      | true == boolean             | Returns TRUE if BOOLEAN is TRUE; returns FALSE if BOOLEAN is FALSE. |
| boolean IS NOT TRUE  | false == boolean            | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE. |

## Arithmetic Functions

| Function             | Janino Code                 | Description                                                     |
|----------------------|-----------------------------|-----------------------------------------------------------------|
| numeric1 + numeric2  | numeric1 + numeric2         | Returns NUMERIC1 plus NUMERIC2.                                 |
| numeric1 - numeric2  | numeric1 - numeric2         | Returns NUMERIC1 minus NUMERIC2.                                |
| numeric1 * numeric2  | numeric1 * numeric2         | Returns NUMERIC1 multiplied by NUMERIC2.                        |
| numeric1 / numeric2  | numeric1 / numeric2         | Returns NUMERIC1 divided by NUMERIC2.                          |
| numeric1 % numeric2  | numeric1 % numeric2         | Returns the remainder (modulus) of numeric1 divided by numeric2. |
| ABS(numeric)         | abs(numeric)                | Returns the absolute value of numeric.                          |
| CEIL(numeric)        | ceil(numeric)               | Rounds numeric up, and returns the smallest number that is greater than or equal to numeric. |
| FLOOR(numeric)       | floor(numeric)              | Rounds numeric down, and returns the largest number that is less than or equal to numeric. |
| ROUND(numeric, int)  | round(numeric)              | Returns a number rounded to INT decimal places for NUMERIC.     |
| UUID()               | uuid()                      | Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. |

## String Functions

| Function             | Janino Code              | Description                                       |
| -------------------- | ------------------------ | ------------------------------------------------- |
| string1 &#124;&#124; string2 | concat(string1, string2) | Returns the concatenation of STRING1 and STRING2. |
| CHAR_LENGTH(string)  | charLength(string)       | Returns the number of characters in STRING.       |
| UPPER(string)        | upper(string)            | Returns string in uppercase.                      |
| LOWER(string) | lower(string) | Returns string in lowercase. |
| TRIM(string1) | trim('BOTH',string1) | Returns a string that removes whitespaces at both sides. |
| REGEXP_REPLACE(string1, string2, string3) | regexpReplace(string1, string2, string3) | Returns a string from STRING1 with all the substrings that match a regular expression STRING2 consecutively being replaced with STRING3. E.g., 'foobar'.regexpReplace('oo\|ar', '') returns "fb". |
| SUBSTRING(string FROM integer1 [ FOR integer2 ]) | substring(string,integer1,integer2) | Returns a substring of STRING starting from position INT1 with length INT2 (to the end by default). |
| CONCAT(string1, string2,…) | concat(string1, string2,…) | Returns a string that concatenates string1, string2, …. E.g., CONCAT('AA', 'BB', 'CC') returns 'AABBCC'. |

## Temporal Functions

| Function             | Janino Code              | Description                                       |
| -------------------- | ------------------------ | ------------------------------------------------- |
| LOCALTIME | localtime() | Returns the current SQL time in the local time zone, the return type is TIME(0). |
| LOCALTIMESTAMP | localtimestamp() | Returns the current SQL timestamp in local time zone, the return type is TIMESTAMP(3). |
| CURRENT_TIME | currentTime() | Returns the current SQL time in the local time zone, this is a synonym of LOCAL_TIME. |
| CURRENT_DATE                                         | currentDate() | Returns the current SQL date in the local time zone. |
| CURRENT_TIMESTAMP | currentTimestamp() | Returns the current SQL timestamp in the local time zone, the return type is TIMESTAMP_LTZ(3). |
| NOW() | now() | Returns the current SQL timestamp in the local time zone, this is a synonym of CURRENT_TIMESTAMP. |
| DATE_FORMAT(timestamp, string) | dateFormat(timestamp, string) | Converts timestamp to a value of string in the format specified by the date format string. The format string is compatible with Java's SimpleDateFormat. |
| TIMESTAMPDIFF(timepointunit, timepoint1, timepoint2) | timestampDiff(timepointunit, timepoint1, timepoint2) | Returns the (signed) number of timepointunit between timepoint1 and timepoint2. The unit for the interval is given by the first argument, which should be one of the following values: SECOND, MINUTE, HOUR, DAY, MONTH, or YEAR. |
| TO_DATE(string1[, string2]) | toDate(string1[, string2]) | Converts a date string string1 with format string2 (by default 'yyyy-MM-dd') to a date. |
| TO_TIMESTAMP(string1[, string2]) | toTimestamp(string1[, string2]) | Converts date time string string1 with format string2 (by default: 'yyyy-MM-dd HH:mm:ss') to a timestamp, without time zone. |

## Conditional Functions

| Function             | Janino Code              | Description                                       |
| -------------------- | ------------------------ | ------------------------------------------------- |
| CASE value WHEN value1_1 [, value1_2]* THEN RESULT1 (WHEN value2_1 [, value2_2 ]* THEN result_2)* (ELSE result_z) END | Nested ternary expression | Returns resultX when the first time value is contained in (valueX_1, valueX_2, …). When no value matches, returns result_z if it is provided and returns NULL otherwise. |
| CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END | Nested ternary expression | Returns resultX when the first conditionX is met. When no condition is met, returns result_z if it is provided and returns NULL otherwise. |
| COALESCE(value1 [, value2]*) | coalesce(Object... objects) | Returns the first argument that is not NULL.If all arguments are NULL, it returns NULL as well. The return type is the least restrictive, common type of all of its arguments. The return type is nullable if all arguments are nullable as well. |
| IF(condition, true_value, false_value)   | condition ? true_value : false_value | Returns the true_value if condition is met, otherwise false_value. E.g., IF(5 > 3, 5, 3) returns 5. |

# Example
## Add computed columns
If add two computed columns to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name, localtimestamp as new_timestamp
    description: project fields from source table
```

## Reference metadata columns
If add a metadata columns to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name
    description: project fields from source table
```

## Use star instead of all fields
If use star instead of all fields in the table `web_order` and  `app_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: \*, UPPER(product_name) as product_name
    description: project fields from source table
  - source-table: mydb.app_order
    projection: UPPER(product_name) as product_name, *
    description: project fields from source table
```
Tips: When the star is at the beginning of the rule, a backslash needs to be added for escape.

## Add filter rule
Use reference columns when adding filtering rules to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    filter: id > 10 AND order_id > 100
    description: project fields from source table
```

Use computed columns when adding filtering rules to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(province) as new_province 
    filter: new_province = 'SHANGHAI'
    description: project fields from source table
```

## Reassign primary key
If reassign primary key to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id
    primary-keys: order_id
    description: project fields from source table
```

If reassign composite primary key to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    primary-keys: order_id,product_name
    description: project fields from source table
```

## Reassign partition key
If reassign partition key to the table `web_order` in the database `mydb`, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    partition-keys: product_name
    description: project fields from source table
```

## Specify table creation configuration
If specify the options to the table `web_order` in the database `mydb` when auto create table, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    table-options: comment=web order
    description: project fields from source table
```
Tips: The format of table-options is `key1=value1,key2=value2`.

## Classification mapping

Perform different processing on different classification data within a table, we can use this yaml file to define this transform：

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id
    filter: UPPER(province) = 'SHANGHAI'
    description: project fields from source table
  - source-table: mydb.web_order
    projection: order_id as id, id as order_id
    filter: UPPER(province) = 'BEIJING'
    description: project fields from source table
```

# Problem
· Transform cannot be used with route in the current version, future versions will support it.
· The computed column does not support referencing the trimmed original column in the current version, future versions will support it.