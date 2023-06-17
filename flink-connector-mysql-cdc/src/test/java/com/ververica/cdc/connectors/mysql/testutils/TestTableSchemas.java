/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.testutils;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
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
}
