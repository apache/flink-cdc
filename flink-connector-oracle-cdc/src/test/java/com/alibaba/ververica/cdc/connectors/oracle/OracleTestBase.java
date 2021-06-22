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

package com.alibaba.ververica.cdc.connectors.oracle;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Basic class for testing Oracle logminer source, this contains a Oracle container which enables
 * logminer.
 */
public abstract class OracleTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final DockerImageName OC_IMAGE =
            DockerImageName.parse("thake/oracle-xe-11g").asCompatibleSubstituteFor("oracle");

    protected static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer(OC_IMAGE)
                    .withClasspathResourceMapping(
                            "docker/setup.sh", "/etc/logminer_conf.sh", BindMode.READ_WRITE)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");

        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                ORACLE_CONTAINER.getJdbcUrl(),
                ORACLE_CONTAINER.getUsername(),
                ORACLE_CONTAINER.getPassword());
    }
}
