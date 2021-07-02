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
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link MySQLSnapshotSplitAssigner}.
 */
public class MySQLSnapshotSplitAssignerTest extends MySQLTestBase {

    private static final UniqueDatabase customDatabase =
        new UniqueDatabase(MYSQL_CONTAINER, "custom", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customDatabase.createAndInitialize();
    }

    @Test
    public void testAssignSingleTableSplits() {
        String[] expected = new String[]{
            "customers SNAPSHOT null [109]",
            "customers SNAPSHOT [109] [118]",
            "customers SNAPSHOT [118] [1009]",
            "customers SNAPSHOT [1009] [1012]",
            "customers SNAPSHOT [1012] [1015]",
            "customers SNAPSHOT [1015] [1018]",
            "customers SNAPSHOT [1018] [2000]",
            "customers SNAPSHOT [2000] null"
        };
        List<String> splits = testAssignSnapshotSplits(new String[]{"customers"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignMultipleTableSplits() {
        String[] expected = new String[]{
            "customers SNAPSHOT null [109]",
            "customers SNAPSHOT [109] [118]",
            "customers SNAPSHOT [118] [1009]",
            "customers SNAPSHOT [1009] [1012]",
            "customers SNAPSHOT [1012] [1015]",
            "customers SNAPSHOT [1015] [1018]",
            "customers SNAPSHOT [1018] [2000]",
            "customers SNAPSHOT [2000] null",
            "customers_1 SNAPSHOT null [109]",
            "customers_1 SNAPSHOT [109] [118]",
            "customers_1 SNAPSHOT [118] [1009]",
            "customers_1 SNAPSHOT [1009] [1012]",
            "customers_1 SNAPSHOT [1012] [1015]",
            "customers_1 SNAPSHOT [1015] [1018]",
            "customers_1 SNAPSHOT [1018] [2000]",
            "customers_1 SNAPSHOT [2000] null"
        };
        List<String> splits = testAssignSnapshotSplits(new String[]{"customers", "customers_1"});
        assertArrayEquals(expected, splits.toArray());
    }

    private List<String> testAssignSnapshotSplits(String[] captureTables) {
        Configuration configuration = getConfig();
        List<String> captureTableIds = Arrays.stream(captureTables)
            .map(tableName -> customDatabase.getDatabaseName() + "." + tableName)
            .collect(Collectors.toList());
        configuration.setString("table.whitelist", String.join(",", captureTableIds));

        final RowType pkType =
            (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        MySQLSnapshotSplitAssigner assigner =
            new MySQLSnapshotSplitAssigner(
                configuration, pkType, new ArrayList<>(), new ArrayList<>());

        assigner.open();
        List<MySQLSplit> sqlSplits = new ArrayList<>();
        while (true) {
            Optional<MySQLSplit> mySQLSplit = assigner.getNext(null);
            if (mySQLSplit.isPresent()) {
                sqlSplits.add(mySQLSplit.get());
            } else {
                break;
            }
        }

        return sqlSplits.stream().map(
            split ->
                split.getTableId().table() + " " +
                    split.getSplitKind() + " " +
                    Arrays.toString(split.getSplitBoundaryStart()) + " " +
                    Arrays.toString(split.getSplitBoundaryEnd())
        ).collect(Collectors.toList());
    }

    private Configuration getConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.whitelist", customDatabase.getDatabaseName());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id.range", "1001,1004");
        properties.put("scan.split.size", "4");
        properties.put("scan.fetch.size", "2");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", DATABASE_HISTORY_INSTANCE_NAME);
        return Configuration.fromMap(properties);
    }
}
