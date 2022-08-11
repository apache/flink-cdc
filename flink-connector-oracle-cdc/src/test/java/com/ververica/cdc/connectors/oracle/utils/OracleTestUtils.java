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

package com.ververica.cdc.connectors.oracle.utils;

import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Utility class for oracle tests. */
public class OracleTestUtils {

    // You can build OracleContainer from official oracle docker image in following way, we use
    // prebuilt image for time cost consideration
    // ----------------- begin --------------------------
    // new OracleContainer(new ImageFromDockerfile("oracle-xe-11g-tmp")
    //                          .withFileFromClasspath(".", "docker")
    //                          .withFileFromClasspath(
    //                                  "assets/activate-archivelog.sh",
    //                                  "docker/assets/activate-archivelog.sh")
    //                          .withFileFromClasspath(
    //                                  "assets/activate-archivelog.sql",
    //                                  "docker/assets/activate-archivelog.sql")
    // ----------------- end --------------------------
    private static final String ORACLE_IMAGE = "jark/oracle-xe-11g-r2-cdc:0.1";

    public static final OracleContainer ORACLE_CONTAINER = new OracleContainer(ORACLE_IMAGE);

    public static final String CONNECTOR_USER = "dbzuser";

    public static final String CONNECTOR_PWD = "dbz";

    public static final String SCHEMA_USER = "debezium";

    public static final String SCHEMA_PWD = "dbz";

    public static Connection getJdbcConnection(OracleContainer oracleContainer)
            throws SQLException {
        return DriverManager.getConnection(
                oracleContainer.getJdbcUrl(), CONNECTOR_USER, CONNECTOR_PWD);
    }

    public static Connection testConnection(OracleContainer oracleContainer) throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), SCHEMA_USER, SCHEMA_PWD);
    }
}
