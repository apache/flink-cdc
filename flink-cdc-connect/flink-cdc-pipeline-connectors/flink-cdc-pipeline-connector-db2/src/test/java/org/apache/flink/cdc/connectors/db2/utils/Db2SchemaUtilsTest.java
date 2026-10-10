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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class Db2SchemaUtilsTest {

    @Test
    void testQuoteEscapesDoubleQuote() {
        assertThat(Db2SchemaUtils.quote("SCHEMA\"NAME")).isEqualTo("\"SCHEMA\"\"NAME\"");
    }

    @Test
    void testTableIdConversionKeepsDatabaseSchemaAndTable() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId("TESTDB", "DB2INST1", "PRODUCTS");

        TableId cdcTableId = Db2SchemaUtils.toCdcTableId(dbzTableId);

        assertThat(cdcTableId.getNamespace()).isEqualTo("TESTDB");
        assertThat(cdcTableId.getSchemaName()).isEqualTo("DB2INST1");
        assertThat(cdcTableId.getTableName()).isEqualTo("PRODUCTS");
        assertThat(Db2SchemaUtils.toDbzTableId(cdcTableId)).isEqualTo(dbzTableId);
    }

    @Test
    void testToColumnNormalizesDb2DefaultValueExpression() {
        io.debezium.relational.Column dbzColumn =
                io.debezium.relational.Column.editor()
                        .name("STATUS")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR", "VARCHAR")
                        .length(8)
                        .optional(true)
                        .defaultValueExpression("('A''B')")
                        .create();

        Column column = Db2SchemaUtils.toColumn(dbzColumn);

        assertThat(column.getName()).isEqualTo("STATUS");
        assertThat(column.getType()).isEqualTo(DataTypes.VARCHAR(8));
        assertThat(column.getDefaultValueExpression()).isEqualTo("A'B");
    }

    @Test
    void testMetadataErrorMentionsDb2Prerequisites() {
        assertThat(
                        Db2SchemaUtils.db2MetadataError(
                                "list tables", new SQLException("missing ASNCDC")))
                .contains("Failed to list tables")
                .contains("ASNCDC")
                .contains("captured tables")
                .contains("configured user")
                .contains("missing ASNCDC");
    }
}
