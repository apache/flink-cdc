/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.tests;

import com.ververica.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;
import com.ververica.cdc.connectors.tests.utils.JdbcProxy;
import com.ververica.cdc.connectors.tests.utils.TestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** End-to-end tests for mysql-cdc connector uber jar. */
public class MySqlE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlE2eITCase.class);

    private static final Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-connector.jar");

    @Test
    public void testMySqlCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " point_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'mysql-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_MYSQL_ALIAS + "',",
                        " 'port' = '3306',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "',",
                        " 'database-name' = '" + mysqlInventoryDatabase.getDatabaseName() + "',",
                        " 'table-name' = 'products_source',",
                        " 'server-time-zone' = 'UTC',",
                        " 'server-id' = '5800-5900',",
                        " 'scan.incremental.snapshot.chunk.size' = '4'",
                        ");",
                        "CREATE TABLE products_sink (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " point_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = 'products_sink',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT * FROM products_source;");

        submitSQLJob(sqlLines, mysqlCdcJar, jdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE products_source SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products_source SET weight='5.1' WHERE id=107;");
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null);");
            stat.execute(
                    "UPDATE products_source SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products_source SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products_source WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // assert final results
        JdbcProxy proxy =
                new JdbcProxy(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"},{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"},{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"},{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"},{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"},{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106,hammer,18oz carpenter hammer,1.0,null,null,null",
                        "107,rocks,box of assorted rocks,5.1,null,null,null",
                        "108,jacket,water resistent black wind breaker,0.1,null,null,null",
                        "109,spare tire,24 inch spare tire,22.2,null,null,null",
                        "110,jacket,new water resistent white wind breaker,0.5,null,null,null");
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight", "enum_c", "json_c", "point_c"},
                60000L);
    }
}
