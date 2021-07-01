/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.assigner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLTestBase;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for {@link MySQLSnapshotSplitAssigner}. */
public class MySQLSnapshotSplitAssignerTest extends MySQLTestBase {

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "custom", "mysqluser", "mysqlpw");

    @Test
    public void testAssignSnapshotSplits() {
        customDatabase.createAndInitialize();
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id-range", "1001, 1002");
        properties.put("scan.split.size", "10");
        properties.put("scan.fetch.size", "2");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", "flink-embedded-database-history");

        Configuration configuration = Configuration.fromMap(properties);
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        MySQLSnapshotSplitAssigner assigner =
                new MySQLSnapshotSplitAssigner(
                        configuration, pkType, new ArrayList<>(), new ArrayList<>());

        assigner.open();
        List<MySQLSplit> mySQLSplitList = new ArrayList<>();
        while (true) {
            Optional<MySQLSplit> mySQLSplit = assigner.getNext(null);
            if (mySQLSplit.isPresent()) {
                mySQLSplitList.add(mySQLSplit.get());
            } else {
                break;
            }
        }

        String[] expected =
                new String[] {
                    "SNAPSHOT null [1009]",
                    "SNAPSHOT [1009] [1018]",
                    "SNAPSHOT [1018] [2000]",
                    "SNAPSHOT [2000] null"
                };
        assertEquals(expected.length, mySQLSplitList.size());
        String[] actual = new String[expected.length];
        for (int i = 0; i < expected.length; i++) {
            MySQLSplit mySQLSplit = mySQLSplitList.get(i);
            String item =
                    mySQLSplit.getSplitKind()
                            + " "
                            + Arrays.toString(mySQLSplit.getSplitBoundaryStart())
                            + " "
                            + Arrays.toString(mySQLSplit.getSplitBoundaryEnd());
            actual[i] = item;
        }
        assertArrayEquals(expected, actual);
        assigner.close();
    }
}
