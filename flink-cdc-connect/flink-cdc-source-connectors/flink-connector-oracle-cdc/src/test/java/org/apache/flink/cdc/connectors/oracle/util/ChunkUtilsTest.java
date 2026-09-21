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

package org.apache.flink.cdc.connectors.oracle.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ChunkUtils}. */
class ChunkUtilsTest {

    @Test
    void testGetChunkKeyColumnMatchesCaseInsensitive() {
        Table table =
                Table.editor()
                        .tableId(new TableId("ORCL19", "TESTUSER", "ORDERS"))
                        .addColumn(Column.editor().name("ID").jdbcType(Types.NUMERIC).create())
                        .addColumn(
                                Column.editor().name("PRODUCT_ID").jdbcType(Types.NUMERIC).create())
                        .setPrimaryKeyNames("ID")
                        .create();

        Column chunkKeyColumn = ChunkUtils.getChunkKeyColumn(table, "id");
        assertThat(chunkKeyColumn.name()).isEqualTo("ID");
    }
}
