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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ObjectUtils}. */
public class ObjectUtilsTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
    private static MySqlConnection mySqlConnection;

    @BeforeClass
    public static void init() throws SQLException {
        customerDatabase.createAndInitialize();
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customerDatabase.getUsername());
        properties.put("database.password", customerDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        mySqlConnection = DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (mySqlConnection != null) {
            mySqlConnection.close();
        }
    }
/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils}. */
public class ObjectUtilsTest {

    @Test
    public void testMinus() {
        assertEquals(BigDecimal.valueOf(9999), ObjectUtils.minus(10000, 1));
        assertEquals(
                BigDecimal.valueOf(4294967295L),
                ObjectUtils.minus(Integer.MAX_VALUE, Integer.MIN_VALUE));

        assertEquals(BigDecimal.valueOf(9999999999999L), ObjectUtils.minus(10000000000000L, 1L));
        assertEquals(
                new BigDecimal("18446744073709551615"),
                ObjectUtils.minus(Long.MAX_VALUE, Long.MIN_VALUE));

        assertEquals(
                new BigDecimal("99.12344"),
                ObjectUtils.minus(new BigDecimal("100.12345"), new BigDecimal("1.00001")));
    }

    @Test
    public void testDbCompare() throws SQLException {
        TableId tableId =
                new TableId(
                        customerDatabase.getDatabaseName(),
                        null,
                        "unevenly_shopping_cart_utf8_bin");
        int resultBin1 =
                ObjectUtils.dbCompare("aaaa", "zzzz", mySqlConnection, tableId, "pk_varchar");
        int resultBin2 =
                ObjectUtils.dbCompare("zzzz", "ZZZZ", mySqlConnection, tableId, "pk_varchar");
        int resultBin3 = ObjectUtils.dbCompare("9", "a", mySqlConnection, tableId, "pk_varchar");
        Assertions.assertEquals(-1, resultBin1);
        Assertions.assertEquals(1, resultBin2);
        Assertions.assertEquals(-1, resultBin3);

        TableId tableId2 =
                new TableId(
                        customerDatabase.getDatabaseName(), null, "unevenly_shopping_cart_utf8_ci");
        int resultCi1 =
                ObjectUtils.dbCompare("aaaa", "zzzz", mySqlConnection, tableId2, "pk_varchar");
        int resultCi2 =
                ObjectUtils.dbCompare("zzzz", "ZZZZ", mySqlConnection, tableId2, "pk_varchar");
        int resultCi3 =
                ObjectUtils.dbCompare("ZZZZ", "aaaa", mySqlConnection, tableId2, "pk_varchar");
        int resultCi4 = ObjectUtils.dbCompare("9", "a", mySqlConnection, tableId2, "pk_varchar");
        Assertions.assertEquals(-1, resultCi1);
        Assertions.assertEquals(0, resultCi2);
        Assertions.assertEquals(1, resultCi3);
        Assertions.assertEquals(-1, resultCi4);
    }

    public static MySqlSourceConfig getConfig(
            UniqueDatabase database,
            String[] captureTables,
            int splitSize,
            boolean skipSnapshotBackfill) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .serverId("1001-1002")
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(database.getUsername())
                .splitSize(splitSize)
                .fetchSize(2)
                .password(database.getPassword())
                .skipSnapshotBackfill(skipSnapshotBackfill)
                .createConfig(0);
    }
}
