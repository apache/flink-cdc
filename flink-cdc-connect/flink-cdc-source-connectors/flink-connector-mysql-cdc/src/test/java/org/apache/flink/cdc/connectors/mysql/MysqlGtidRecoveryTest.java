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

package org.apache.flink.cdc.connectors.mysql;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test gtid recovery in mysql. */
public class MysqlGtidRecoveryTest extends MySqlSourceTestBase {

    private static final String SCHEMA = "flink-test";
    private static final String USERNAME = "mysqluser";
    private static final String PASSWORD = "mysqlpw";

    @Test
    public void testGtidGapsPreservedDuringRecovery() throws Exception {
        // After creation of the table GTID Set will be <gtid>:1-23
        createTableAndFillWithData();
        String serverUuid = getServerUuid();

        String gtidSetWithGaps = serverUuid + ":1-10:14-16:19-19:21-22";

        List<Integer> expectedReceivedGtids = Arrays.asList(11, 12, 13, 17, 18, 20, 23);
        List<Integer> receivedGtidNumbers = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(expectedReceivedGtids.size());

        BinaryLogClient client =
                new BinaryLogClient(
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getMappedPort(3306),
                        SCHEMA,
                        USERNAME,
                        PASSWORD);

        client.setGtidSet(gtidSetWithGaps);
        client.setServerId(System.currentTimeMillis());

        client.registerEventListener(
                event -> {
                    if (event.getData() instanceof GtidEventData) {
                        String gtid = ((GtidEventData) event.getData()).getGtid();
                        int gtidNum = Integer.parseInt(gtid.split(":")[1]);
                        receivedGtidNumbers.add(gtidNum);
                        latch.countDown();
                    }
                });

        // Connect in background thread, since connect is blocking method
        new Thread(
                        () -> {
                            try {
                                client.connect();
                            } catch (Exception ignored) {
                            }
                        })
                .start();

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        client.disconnect();

        assertEqualsInAnyOrder(expectedReceivedGtids, receivedGtidNumbers);
    }

    private static void createTableAndFillWithData() throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), USERNAME, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY)");
            for (int i = 1; i <= 10; i++) {
                stmt.execute("INSERT INTO test_table VALUES (" + i + ")");
            }
        }
    }

    private String getServerUuid() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), USERNAME, PASSWORD);
                Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT @@server_uuid");
            return rs.next() ? rs.getString(1) : "";
        }
    }
}
