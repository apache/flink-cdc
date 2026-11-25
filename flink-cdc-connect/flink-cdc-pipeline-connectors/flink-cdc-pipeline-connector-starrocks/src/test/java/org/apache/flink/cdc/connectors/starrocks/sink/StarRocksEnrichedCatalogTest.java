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

package org.apache.flink.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StarRocksEnrichedCatalogTest {
    private final StarRocksEnrichedCatalog catalog =
            new StarRocksEnrichedCatalog("jdbc:mysql://localhost:9030", "root", "");

    @Test
    void testReorderSqlEscapesIdentifiersAndPreservesColumnDefinition() {
        StarRocksColumn column =
                new StarRocksColumn.Builder()
                        .setColumnName("a`b")
                        .setOrdinalPosition(1)
                        .setDataType("VARCHAR")
                        .setColumnSize(32)
                        .setNullable(false)
                        .setDefaultValue("a'b")
                        .setColumnComment("c'd")
                        .build();
        assertThat(catalog.buildAlterColumnPositionSql("d`b", "t`b", column, false, "i`d"))
                .isEqualTo(
                        "ALTER TABLE `d``b`.`t``b` MODIFY COLUMN `a``b` VARCHAR(32) NOT NULL DEFAULT 'a''b' COMMENT 'c''d' AFTER `i``d`");
    }

    @Test
    void testReorderSqlKeepsTimestampExpressionButQuotesStringDefaults() {
        StarRocksColumn timestamp =
                new StarRocksColumn.Builder()
                        .setColumnName("ts")
                        .setOrdinalPosition(1)
                        .setDataType("DATETIME")
                        .setNullable(true)
                        .setDefaultValue("CURRENT_TIMESTAMP")
                        .build();
        assertThat(catalog.buildAlterColumnPositionSql("db", "t", timestamp, false, null))
                .endsWith("DATETIME NULL DEFAULT CURRENT_TIMESTAMP FIRST");
        StarRocksColumn string =
                new StarRocksColumn.Builder()
                        .setColumnName("s")
                        .setOrdinalPosition(1)
                        .setDataType("VARCHAR")
                        .setColumnSize(32)
                        .setNullable(true)
                        .setDefaultValue("CURRENT_TIMESTAMP")
                        .build();
        assertThat(catalog.buildAlterColumnPositionSql("db", "t", string, false, null))
                .endsWith("VARCHAR(32) NULL DEFAULT 'CURRENT_TIMESTAMP' FIRST");
    }

    @Test
    void testReorderSqlPreservesKeyAndDecimalPrecision() {
        StarRocksColumn column =
                new StarRocksColumn.Builder()
                        .setColumnName("id")
                        .setOrdinalPosition(0)
                        .setDataType("DECIMAL")
                        .setColumnSize(12)
                        .setDecimalDigits(3)
                        .setNullable(false)
                        .build();
        assertThat(catalog.buildAlterColumnPositionSql("db", "t", column, true, null))
                .isEqualTo(
                        "ALTER TABLE `db`.`t` MODIFY COLUMN `id` DECIMAL(12, 3) KEY NOT NULL FIRST");
    }
}
