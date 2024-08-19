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
To describe a transform rule, the following parameters can be used:

| Parameter    | Meaning                                            | Optional/Required |
|--------------|----------------------------------------------------|-------------------|
| source-table | Source table id, supports regular expressions      | required          |
| projection   | Projection rule, supports syntax similar to the select clause in SQL      | optional          |
| filter       | Filter rule, supports syntax similar to the where clause in SQL      | optional          |
| primary-keys | Sink table primary keys, separated by commas       | optional          |
| partition-keys | Sink table partition keys, separated by commas       | optional          |
| table-options | used to the configure table creation statement when automatically creating tables | optional          |
| description  | Transform rule description | optional          |

Multiple rules can be declared in one single pipeline YAML file.

# Metadata Fields
## Fields definition
There are some hidden columns used to access metadata information. They will only take effect when explicitly referenced in the transform rules.

| Field               | Data Type | Description                                  |
|---------------------|-----------|----------------------------------------------|
| __namespace_name__  | String    | Name of the namespace that contains the row. |
| __schema_name__     | String    | Name of the schema that contains the row.    |
| __table_name__      | String    | Name of the table that contains the row.     |
| __data_event_type__ | String    | Operation type of data change event.         |

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
Flink CDC uses [Calcite](https://calcite.apache.org/) to parse expressions and [Janino script](https://www.janino.net/) to evaluate expressions with function call.

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
Evaluation expressions can be used to generate new columns. For example, if we want to append two computed columns based on the table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name, localtimestamp as new_timestamp
    description: append calculated columns based on source table
```

## Reference metadata columns
We may reference metadata column in projection expressions. For example, given a table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name
    description: access metadata columns from source table
```

## Use wildcard character to project all fields
A wildcard character (`*`) can be used to reference all fields in a table. For example, given two tables `web_order` and `app_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: \*, UPPER(product_name) as product_name
    description: project fields with wildcard character from source table
  - source-table: mydb.app_order
    projection: UPPER(product_name) as product_name, *
    description: project fields with wildcard character from source table
```
Notice: When `*` character presents at the beginning of expressions, an escaping backslash is required.

## Add filter rule
Use reference columns when adding filtering rules to the table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    filter: id > 10 AND order_id > 100
    description: filtering rows from source table
```

Computed columns can be used in filtering conditions, too. For example, given a table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(province) as new_province 
    filter: new_province = 'SHANGHAI'
    description: filtering rows based on computed columns
```

## Reassign primary key
We can reassign the primary key in transform rules. For example, given a table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id
    primary-keys: order_id
    description: reassign primary key example
```

Composite primary keys are also supported:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    primary-keys: order_id, product_name
    description: reassign composite primary keys example
```

## Reassign partition key
We can reassign the partition key in transform rules. For example, given a table web_order in the database mydb, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    partition-keys: product_name
    description: reassign partition key example
```

## Specify table creation configuration
Extra options can be defined in a transform rule, and will be applied when creating downstream tables. Given a table `web_order` in the database `mydb`, we may define a transform rule as follows:

```yaml
transform:
  - source-table: mydb.web_order
    projection: id, order_id, UPPER(product_name) as product_name
    table-options: comment=web order
    description: auto creating table options example
```
Tips: The format of table-options is `key1=value1,key2=value2`.

## Classification mapping
Multiple transform rules can be defined to classify input data rows and apply different processing.
Only the first matched transform rule will apply.
For example, we may define a transform rule as follows:

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

## User-defined Functions

User-defined functions (UDFs) can be used in transform rules.

Classes could be used as a UDF if:

* implements `org.apache.flink.cdc.common.udf.UserDefinedFunction` interface
* has a public constructor with no parameters
* has at least one public method named `eval`

It may also:

* overrides `getReturnType` method to indicate its return CDC type
* overrides `open` and `close` method to do some initialization and cleanup work

For example, this is a valid UDF class:

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

To ease the migration from Flink SQL to Flink CDC, a Flink `ScalarFunction` could also be used as a transform UDF, with some limitations:

* `ScalarFunction` which has a constructor with parameters is not supported.
* Flink-style type hint in `ScalarFunction` will be ignored.
* `open` / `close` lifecycle hooks will not be invoked.

UDF classes could be registered by adding a `user-defined-function` block:

```yaml
pipeline:
  user-defined-function:
    - name: addone
      classpath: org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass
    - name: format
      classpath: org.apache.flink.cdc.udf.examples.java.FormatFunctionClass
```

Notice that given classpath must be fully-qualified, and corresponding `jar` files must be included in Flink `/lib` folder, or be passed with `flink-cdc.sh --jar` option.

After being correctly registered, UDFs could be used in both `projection` and `filter` expressions, just like built-in functions:

```yaml
transform:
  - source-table: db.\.*
    projection: "*, inc(inc(inc(id))) as inc_id, format(id, 'id -> %d') as formatted_id"
    filter: inc(id) < 100
```

# Known limitations
* Currently, transform doesn't work with route rules. It will be supported in future versions.
* Computed columns cannot reference trimmed columns that do not present in final projection results. This will be fixed in future versions.
* Regular matching of tables with different schemas is not supported. If necessary, multiple rules need to be written.