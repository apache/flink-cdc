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

package org.apache.flink.cdc.connectors.oceanbase;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Basic class for testing OceanBase. */
public class OceanBaseTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseTestBase.class);

    public static final String IMAGE_TAG = "4.2.1_bp2";

    @ClassRule
    public static final OceanBaseContainer OB_SERVER =
            new OceanBaseContainer(OceanBaseContainer.DOCKER_IMAGE_NAME + ":" + IMAGE_TAG)
                    .withNetworkMode("host")
                    .withSysPassword("123456")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("sql/init.sql"),
                            "/root/boot/init.d/init.sql")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                OB_SERVER.getJdbcUrl(), OB_SERVER.getUsername(), OB_SERVER.getPassword());
    }
}
