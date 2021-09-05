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

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.test.util.AbstractTestBase;

import com.ververica.cdc.connectors.mysql.source.utils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.source.utils.MySqlContainer.MysqlVersion;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Basic class for testing muti-version MySQL binlog source. this contains a mysql container member
 * need to through parameterized setting.
 */
public abstract class MySqlMultiVersionTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlMultiVersionTestBase.class);

    // Cache already created mysql container
    private static final Map<String, MySqlContainer> MYSQL_CONTAINER_CACHE = new HashMap<>();

    protected MySqlContainer mySqlContainer;

    public MySqlMultiVersionTestBase(MysqlVersion mysqlVersion, boolean enableGtid) {
        this.mySqlContainer =
                MySqlMultiVersionTestBase.createMySqlContainer(mysqlVersion, enableGtid);
    }

    @Parameterized.Parameters(name = "version: {0},enableGtid: {1}")
    public static Collection<Object[]> parameterOfMySqlContainer() {
        return Arrays.asList(
                new Object[][] {
                    {MysqlVersion.V5_6, false},
                    {MysqlVersion.V5_7, false}
                });
    }

    @Before
    public void startContainers() {
        if (mySqlContainer != null && !mySqlContainer.isRunning()) {
            LOG.info("Starting containers...");
            Startables.deepStart(Stream.of(mySqlContainer)).join();
            LOG.info("Containers are started.");
        } else if (mySqlContainer != null && mySqlContainer.isRunning()) {
            LOG.info("Containers are already started.");
        } else {
            LOG.warn("Mysql docker container is null,Please initialization mysql container");
        }
    }

    public static synchronized MySqlContainer createMySqlContainer(MysqlVersion version) {
        return createMySqlContainer(version, false);
    }

    public static synchronized MySqlContainer createMySqlContainer(
            MysqlVersion version, boolean enableGtid) {
        MySqlContainer mySqlContainer =
                MYSQL_CONTAINER_CACHE.get(version.getVersion() + "#" + enableGtid);
        if (mySqlContainer == null) {
            mySqlContainer =
                    (MySqlContainer)
                            new MySqlContainer(version)
                                    .withConfigurationOverride("docker/my.cnf")
                                    .withSetupSQL("docker/setup.sql")
                                    .withDatabaseName("flink-test")
                                    .withUsername("flinkuser")
                                    .withPassword("flinkpw")
                                    .withEnableGtid(enableGtid)
                                    .withLogConsumer(new Slf4jLogConsumer(LOG));
            MYSQL_CONTAINER_CACHE.put(version.getVersion() + "#" + enableGtid, mySqlContainer);
        }
        return mySqlContainer;
    }
}
