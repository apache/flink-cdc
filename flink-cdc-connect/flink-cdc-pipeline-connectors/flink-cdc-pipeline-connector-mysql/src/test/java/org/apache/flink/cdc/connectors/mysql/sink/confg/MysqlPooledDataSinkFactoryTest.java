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

package org.apache.flink.cdc.connectors.mysql.sink.confg;

import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.mysql.sink.MysqlPooledDataSinkFactory;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test class for {@link MysqlPooledDataSinkFactory}. */
public class MysqlPooledDataSinkFactoryTest {

    /** Test the getJdbcUrl method with basic properties. */
    @Test
    public void testGetJdbcUrlWithBasicProperties() {
        Properties props = new Properties();
        JdbcSinkConfig config =
                new JdbcSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(props)
                        .build();

        MysqlPooledDataSinkFactory factory = new MysqlPooledDataSinkFactory();
        String expectedUrl = "jdbc:mysql://localhost:3306?serverTimezone=UTC";
        String actualUrl = factory.getJdbcUrl(config);
        assertEquals(expectedUrl, actualUrl);
    }

    /** Test the getJdbcUrl method with additional properties. */
    @Test
    public void testGetJdbcUrlWithAdditionalProperties() {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("allowPublicKeyRetrieval", "true");

        JdbcSinkConfig config =
                new JdbcSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(props)
                        .build();

        MysqlPooledDataSinkFactory factory = new MysqlPooledDataSinkFactory();
        String expectedUrl =
                "jdbc:mysql://localhost:3306?serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=true";
        String actualUrl = factory.getJdbcUrl(config);
        assertEquals(expectedUrl, actualUrl);
    }

    /** Test the getJdbcUrl method with empty database name. */
    @Test
    public void testGetJdbcUrlWithEmptyDatabase() {
        Properties props = new Properties();
        JdbcSinkConfig config =
                new JdbcSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(props)
                        .build();

        MysqlPooledDataSinkFactory factory = new MysqlPooledDataSinkFactory();
        String expectedUrl = "jdbc:mysql://localhost:3306?serverTimezone=UTC";
        String actualUrl = factory.getJdbcUrl(config);
        assertEquals(expectedUrl, actualUrl);
    }

    /** Test the getJdbcUrl method with null properties. */
    @Test
    public void testGetJdbcUrlWithNullProperties() {
        JdbcSinkConfig config =
                new JdbcSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(null)
                        .build();

        MysqlPooledDataSinkFactory factory = new MysqlPooledDataSinkFactory();
        String expectedUrl = "jdbc:mysql://localhost:3306?serverTimezone=UTC";
        String actualUrl = factory.getJdbcUrl(config);
        assertEquals(expectedUrl, actualUrl);
    }

    /** Test the getJdbcUrl method with special characters in database name. */
    @Test
    public void testGetJdbcUrlWithSpecialCharacters() {
        Properties props = new Properties();
        JdbcSinkConfig config =
                new JdbcSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(props)
                        .build();

        MysqlPooledDataSinkFactory factory = new MysqlPooledDataSinkFactory();
        String expectedUrl = "jdbc:mysql://localhost:3306?serverTimezone=UTC";
        String actualUrl = factory.getJdbcUrl(config);
        assertEquals(expectedUrl, actualUrl);
    }
}
