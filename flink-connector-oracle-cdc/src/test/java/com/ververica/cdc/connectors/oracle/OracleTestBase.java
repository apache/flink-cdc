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

package com.ververica.cdc.connectors.oracle;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

/** Basic class for testing Oracle source, this contains a Oracle container which enables binlog. */
public class OracleTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceTest.class);

    private static final String ORACLE_USER = "dbzuser";
    private static final String ORACLE_PWD = "dbz";
    protected static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer(
                    new ImageFromDockerfile("oracle-xe-11g-tmp")
                            .withFileFromClasspath(".", "docker")
                            .withFileFromClasspath(
                                    "assets/activate-archivelog.sh",
                                    "docker/assets/activate-archivelog.sh")
                            .withFileFromClasspath(
                                    "assets/activate-archivelog.sql",
                                    "docker/assets/activate-archivelog.sql"));

    @BeforeClass
    public static void beforeClass() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void teardownClass() {
        ORACLE_CONTAINER.stop();
    }

    protected Connection getJdbcConnection(OracleContainer oracleContainer) throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), ORACLE_USER, ORACLE_PWD);
    }
}
