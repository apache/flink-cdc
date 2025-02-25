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

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.flink.cdc.common.schema.Selectors}. */
class SelectorsTest {

    @Test
    void testTableSelector() {

        // nameSpace, schemaName, tableName
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db.sc1.A[0-9]+,db.sc2.B[0-1]+,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db", "sc1", "A1");
        assertAllowed(selectors, "db", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "db2", "sc1", "A1");
        assertNotAllowed(selectors, "db2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db\\..sc1.A[0-9]+,db.sc2.B[0-1]+,db\\..sc1.sc1,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "sc1");
        assertAllowed(selectors, "dba", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "A1");
        assertAllowed(selectors, "dba", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "dba1", "sc1", "A1");
        assertNotAllowed(selectors, "dba2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        // schemaName, tableName
        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertAllowed(selectors, null, "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        // tableName
        selectors =
                new Selectors.SelectorsBuilder().includeTables("\\.A[0-9]+,B[0-1]+,sc1").build();

        assertAllowed(selectors, null, null, "sc1");
        assertNotAllowed(selectors, "db", "sc1", "sc1");
        assertNotAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, null, "1A1");
        assertAllowed(selectors, null, null, "AA2");
        assertAllowed(selectors, null, null, "B0");
        assertAllowed(selectors, null, null, "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, null, "A");
        assertNotAllowed(selectors, null, null, "B");
        assertNotAllowed(selectors, null, null, "2B");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc2", "B2");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc1").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc[0-9]+").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.\\.*").build();
        assertAllowed(selectors, null, "sc1", "sc1");
    }

    protected void assertAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isTrue();
    }

    protected void assertNotAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isFalse();
    }

    private static TableId getTableId(String nameSpace, String schemaName, String tableName) {
        TableId id;
        if (nameSpace == null && schemaName == null) {
            id = TableId.tableId(tableName);
        } else if (nameSpace == null) {
            id = TableId.tableId(schemaName, tableName);
        } else {
            id = TableId.tableId(nameSpace, schemaName, tableName);
        }
        return id;
    }
}
