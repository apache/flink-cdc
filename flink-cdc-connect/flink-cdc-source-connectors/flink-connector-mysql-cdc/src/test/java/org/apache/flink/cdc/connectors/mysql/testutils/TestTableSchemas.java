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

package org.apache.flink.cdc.connectors.mysql.testutils;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.catalog.Column.physical;

/** Predefined schemas for test tables. */
public class TestTableSchemas {
    public static final ResolvedSchema CUSTOMERS =
            new ResolvedSchema(
                    Arrays.asList(
                            physical("id", BIGINT().notNull()),
                            physical("name", STRING()),
                            physical("address", STRING()),
                            physical("phone_number", STRING())),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    public static final ResolvedSchema PRODUCTS =
            new ResolvedSchema(
                    Arrays.asList(
                            physical("id", INT().notNull()),
                            physical("name", STRING().notNull()),
                            physical("description", STRING()),
                            physical("weight", FLOAT())),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    public static final ResolvedSchema SHOPPING_CART =
            new ResolvedSchema(
                    Arrays.asList(
                            physical("product_no", BIGINT().notNull()),
                            physical("product_kind", STRING()),
                            physical("user_id", STRING()),
                            physical("description", STRING())),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("product_no")));

    public static final ResolvedSchema FULL_TYPES =
            new ResolvedSchema(
                    Arrays.asList(
                            physical("id", DECIMAL(20, 0)),
                            physical("tiny_c", TINYINT()),
                            physical("tiny_un_c", SMALLINT()),
                            physical("tiny_un_z_c", SMALLINT()),
                            physical("small_c", SMALLINT()),
                            physical("small_un_c", INT()),
                            physical("small_un_z_c", INT()),
                            physical("medium_c", INT()),
                            physical("medium_un_c", BIGINT()),
                            physical("medium_un_z_c", BIGINT()),
                            physical("int_c", INT()),
                            physical("int_un_c", BIGINT()),
                            physical("int_un_z_c", BIGINT()),
                            physical("int11_c", INT()),
                            physical("big_c", BIGINT()),
                            physical("big_un_c", DECIMAL(20, 0)),
                            physical("big_un_z_c", DECIMAL(20, 0)),
                            physical("varchar_c", VARCHAR(255)),
                            physical("char_c", VARCHAR(3)),
                            physical("real_c", DOUBLE()),
                            physical("float_c", FLOAT()),
                            physical("float_un_c", FLOAT()),
                            physical("float_un_z_c", FLOAT()),
                            physical("double_c", DOUBLE()),
                            physical("double_un_c", DOUBLE()),
                            physical("double_un_z_c", DOUBLE()),
                            physical("decimal_c", DECIMAL(8, 4)),
                            physical("decimal_un_c", DECIMAL(8, 4)),
                            physical("decimal_un_z_c", DECIMAL(8, 4)),
                            physical("numeric_c", DECIMAL(6, 0)),
                            physical("big_decimal_c", STRING()),
                            physical("bit1_c", BOOLEAN()),
                            physical("tiny1_c", BOOLEAN()),
                            physical("boolean_c", BOOLEAN()),
                            physical("date_c", DATE()),
                            physical("time_c", TIME()),
                            physical("datetime3_c", TIMESTAMP(3)),
                            physical("datetime6_c", TIMESTAMP(6)),
                            physical("timestamp_c", TIMESTAMP_LTZ(0)),
                            physical("file_uuid", BINARY(16)),
                            physical("bit_c", BINARY(8)),
                            physical("text_c", STRING()),
                            physical("tiny_blob_c", BYTES()),
                            physical("blob_c", BYTES()),
                            physical("medium_blob_c", BYTES()),
                            physical("long_blob_c", BYTES()),
                            physical("year_c", INT()),
                            physical("enum_c", STRING()),
                            physical("set_c", STRING()),
                            physical("json_c", STRING()),
                            physical("point_c", STRING()),
                            physical("geometry_c", STRING()),
                            physical("linestring_c", STRING()),
                            physical("polygon_c", STRING()),
                            physical("multipoint_c", STRING()),
                            physical("multiline_c", STRING()),
                            physical("multipolygon_c", STRING()),
                            physical("geometrycollection_c", STRING())),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
}
