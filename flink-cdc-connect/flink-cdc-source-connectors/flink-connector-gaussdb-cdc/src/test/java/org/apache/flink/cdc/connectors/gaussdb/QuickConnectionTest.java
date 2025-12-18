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

package org.apache.flink.cdc.connectors.gaussdb;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Quick connectivity test for GaussDB that can be run standalone. */
class QuickConnectionTest {

    private static final String HOSTNAME = "10.250.0.51";
    private static final int PORT = 8000;
    private static final String DATABASE = "db1";
    private static final String USERNAME = "tom";
    private static final String PASSWORD = "Gauss_235";

    @Test
    void testGaussDBDriverConnection() throws Exception {
        System.out.println("=== Testing GaussDB Driver Connection ===");

        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOSTNAME, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD)) {
            assertThat(conn).isNotNull();
            assertThat(conn.isValid(5)).isTrue();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT version()")) {
                assertThat(rs.next()).isTrue();
                String version = rs.getString(1);
                System.out.println("Connected! Version: " + version);
                assertThat(version).containsIgnoringCase("gaussdb");
            }
        }
        System.out.println("GaussDB driver connection: SUCCESS");
    }

    @Test
    void testPostgreSQLDriverConnection() throws Exception {
        System.out.println("=== Testing PostgreSQL Driver Connection ===");

        Class.forName("org.postgresql.Driver");
        String url =
                String.format(
                        "jdbc:postgresql://%s:%d/%s?sslmode=disable", HOSTNAME, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD)) {
            assertThat(conn).isNotNull();
            assertThat(conn.isValid(5)).isTrue();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertThat(rs.next()).isTrue();
                System.out.println("Connected using PostgreSQL driver!");
            }
        }
        System.out.println("PostgreSQL driver connection: SUCCESS");
    }

    @Test
    void testPostgreSQLDriverReplicationMode() throws Exception {
        System.out.println("=== Testing PostgreSQL Driver in Replication Mode ===");

        Class.forName("org.postgresql.Driver");
        String url = String.format("jdbc:postgresql://%s:%d/%s", HOSTNAME, PORT, DATABASE);

        Properties props = new Properties();
        props.setProperty("user", USERNAME);
        props.setProperty("password", PASSWORD);
        props.setProperty("replication", "database");
        props.setProperty("preferQueryMode", "simple");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertThat(conn).isNotNull();

            // Try to unwrap to PGConnection (required for replication API)
            org.postgresql.PGConnection pgConn = conn.unwrap(org.postgresql.PGConnection.class);
            assertThat(pgConn).isNotNull();
            System.out.println(
                    "PGConnection unwrapped successfully, backend PID: " + pgConn.getBackendPID());
        }
        System.out.println("PostgreSQL replication mode connection: SUCCESS");
    }
}
