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

# 类型映射

对于每个 CDC 数据类型（`org.apache.flink.cdc.common.types.DataType` 的子类），我们规定了用于内部序列化和反序列化的 CDC 内部类型，以及用于类型合并、类型转换和 UDF 求值的 Java 外部类型。

## 内部类型和外部类型

一些基本类型对于内部类型和 Java 类可能具有相同的表示形式（例如，`DataTypes.INT()` 使用 `java.lang.Integer` 同时作为内部和外部类型）。
其他类型可能使用不同的表示形式，例如，`DataTypes.TIMESTAMP` 在内部表示中使用 `org.apache.flink.cdc.common.data.TimestampData`，在外部操作中使用 `java.time.LocalDateTime`。

如果您正在编写 YAML Pipeline 连接器，`DataChangeEvent` 应当携带内部类型 `RecordData`，并且其所有字段都是内部类型的实例。

如果您正在编写 Transform UDF，则其参数和返回值类型应定义为其外部 Java 类型。

## 完整类型列表

| CDC 数据类型                       | CDC 内部类型                                                   | Java 外部类型                 |
|--------------------------------|------------------------------------------------------------|---------------------------|
| BOOLEAN                        | `java.lang.Boolean`                                        | `java.lang.Boolean`       |
| TINYINT                        | `java.lang.Byte`                                           | `java.lang.Byte`          |
| SMALLINT                       | `java.lang.Short`                                          | `java.lang.Short`         |
| INTEGER                        | `java.lang.Integer`                                        | `java.lang.Integer`       |
| BIGINT                         | `java.lang.Long`                                           | `java.lang.Long`          |
| FLOAT                          | `java.lang.Float`                                          | `java.lang.Float`         |
| DOUBLE                         | `java.lang.Double`                                         | `java.lang.Double`        |
| DECIMAL                        | `org.apache.flink.cdc.common.data.DecimalData`             | `java.math.BigDecimal`    |
| DATE                           | `org.apache.flink.cdc.common.data.DateData`                | `java.time.LocalDate`     |
| TIME                           | `org.apache.flink.cdc.common.data.TimeData`                | `java.time.LocalTime`     |
| TIMESTAMP                      | `org.apache.flink.cdc.common.data.TimestampData`           | `java.time.LocalDateTime` |
| TIMESTAMP_TZ                   | `org.apache.flink.cdc.common.data.ZonedTimestampData`      | `java.time.ZonedDateTime` |
| TIMESTAMP_LTZ                  | `org.apache.flink.cdc.common.data.LocalZonedTimestampData` | `java.time.Instant`       |
| CHAR<br/>VARCHAR<br/>STRING    | `org.apache.flink.cdc.common.data.StringData`              | `java.lang.String`        |
| BINARY<br/>VARBINARY<br/>BYTES | `byte[]`                                                   | `byte[]`                  |
| ARRAY                          | `org.apache.flink.cdc.common.data.ArrayData`               | `java.util.List<T>`       |
| MAP                            | `org.apache.flink.cdc.common.data.MapData`                 | `java.util.Map<K, V>`     |
| ROW                            | `org.apache.flink.cdc.common.data.RecordData`              | `java.util.List<Object>`  |
