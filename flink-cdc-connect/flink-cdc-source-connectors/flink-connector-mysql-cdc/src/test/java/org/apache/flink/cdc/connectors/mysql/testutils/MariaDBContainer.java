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

package org.apache.flink.cdc.connectors.mysql.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** Docker container for MariaDB. */
public class MariaDBContainer extends org.testcontainers.containers.MariaDBContainer {

    protected static final Logger LOG = LoggerFactory.getLogger(MariaDBContainer.class);

    public MariaDBContainer(String dockerImageName) {
        super(dockerImageName);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(getDatabaseName());
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getMappedPort(3306)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public static MariaDBContainer createMariaDBContainer() {
        MariaDBContainer mariaDBContainer =
                (MariaDBContainer)
                        new MariaDBContainer("mariadb:11.5-rc")
                                .withConfigurationOverride("docker/server")
                                .withDatabaseName("flink-test")
                                .withUsername("flinkuser")
                                .withPassword("flinkpwd")
                                .withEnv("MYSQL_ROOT_PASSWORD", "123456")
                                .withLogConsumer(new Slf4jLogConsumer(LOG));
        return mariaDBContainer;
    }
}
