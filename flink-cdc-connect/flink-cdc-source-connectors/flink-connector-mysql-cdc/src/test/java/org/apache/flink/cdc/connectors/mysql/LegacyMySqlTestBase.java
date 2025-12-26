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

import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.test.util.AbstractTestBase;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * Basic class for testing MySQL binlog source, this contains a MySQL container which enables
 * binlog.
 */
public abstract class LegacyMySqlTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyMySqlTestBase.class);

    protected static final MySqlContainer MYSQL_CONTAINER =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride("server/my.cnf")
                            .withSetupSQL("setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    static void startContainers() {
        // Legacy mode does not support MySQL > 8.0.x
        Assumptions.assumeThat(MySqlVersion.CURRENT).isLessThanOrEqualTo(MySqlVersion.V8_0);
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }
}
