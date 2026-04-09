---
title: "Type Mappings"
weight: 8
type: docs
aliases:
  - /core-concept/type-mappings/
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

# Type Mappings

For each CDC DataType (subclasses of `org.apache.flink.cdc.common.types.DataType`), we define CDC Internal types for internal serialization & deserialization and Java classes (for type merging, casting, and UDF evaluations).

## Internal and External Types

Primitive types may have the same representation for Internal type and Java classes (`DataTypes.INT()` only uses `java.lang.Integer`).
Other types use different representations, like `DataTypes.TIMESTAMP` uses `org.apache.flink.cdc.common.data.TimestampData` for internal representation and `java.time.LocalDateTime` for external operations.

If you're writing a pipeline source / sink connector, `DataChangeEvent` carries internal type `RecordData`, and all its fields are internal type instances.

If you're writing a UDF, its arguments and return value types should be defined as its external Java type.

## Full Types List

| CDC Data Type                  | CDC Internal Type                                          | External Java Class                                 |
|--------------------------------|------------------------------------------------------------|-----------------------------------------------------|
| BOOLEAN                        | `java.lang.Boolean`                                        | `java.lang.Boolean`                                 |
| TINYINT                        | `java.lang.Byte`                                           | `java.lang.Byte`                                    |
| SMALLINT                       | `java.lang.Short`                                          | `java.lang.Short`                                   |
| INTEGER                        | `java.lang.Integer`                                        | `java.lang.Integer`                                 |
| BIGINT                         | `java.lang.Long`                                           | `java.lang.Long`                                    |
| FLOAT                          | `java.lang.Float`                                          | `java.lang.Float`                                   |
| DOUBLE                         | `java.lang.Double`                                         | `java.lang.Double`                                  |
| DECIMAL                        | `org.apache.flink.cdc.common.data.DecimalData`             | `java.math.BigDecimal`                              |
| DATE                           | `org.apache.flink.cdc.common.data.DateData`                | `java.time.LocalDate`                               |
| TIME                           | `org.apache.flink.cdc.common.data.TimeData`                | `java.time.LocalTime`                               |
| TIMESTAMP                      | `org.apache.flink.cdc.common.data.TimestampData`           | `java.time.LocalDateTime`                           |
| TIMESTAMP_TZ                   | `org.apache.flink.cdc.common.data.ZonedTimestampData`      | `java.time.ZonedDateTime`                           |
| TIMESTAMP_LTZ                  | `org.apache.flink.cdc.common.data.LocalZonedTimestampData` | `java.time.Instant`                                 |
| CHAR<br/>VARCHAR<br/>STRING    | `org.apache.flink.cdc.common.data.StringData`              | `java.lang.String`                                  |
| BINARY<br/>VARBINARY<br/>BYTES | `byte[]`                                                   | `byte[]`                                            |
| ARRAY                          | `org.apache.flink.cdc.common.data.ArrayData`               | `java.util.List<T>`                                 |
| MAP                            | `org.apache.flink.cdc.common.data.MapData`                 | `java.util.Map<K, V>`                               |
| ROW                            | `org.apache.flink.cdc.common.data.RecordData`              | `java.util.List<Object>`                            |
| VARIANT                        | `org.apache.flink.cdc.common.types.variant.Variant`        | `org.apache.flink.cdc.common.types.variant.Variant` |
