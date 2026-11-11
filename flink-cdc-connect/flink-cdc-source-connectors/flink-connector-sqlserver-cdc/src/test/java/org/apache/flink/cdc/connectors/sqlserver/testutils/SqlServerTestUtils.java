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

package org.apache.flink.cdc.connectors.sqlserver.testutils;

import org.awaitility.Awaitility;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/** Utilities for SQL Server test containers. */
public class SqlServerTestUtils {

    private static final String AGENT_STARTING_QUERY =
            "SET NOCOUNT ON; DECLARE @starting INT;"
                    + " EXEC master.dbo.xp_sqlagent_is_starting @starting OUTPUT;"
                    + " SELECT @starting";

    private SqlServerTestUtils() {}

    /**
     * Waits until SQL Server Agent has finished starting. Enabling CDC on a table adds Agent jobs,
     * which fails with error 14258 while the Agent is starting.
     */
    public static void waitForSqlServerAgent(MSSQLServerContainer<?> container) {
        Awaitility.await("SQL Server Agent to start")
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                // xp_sqlagent_is_starting is blocked until the Agent enables 'Agent XPs'.
                .ignoreExceptions()
                .until(() -> !isAgentStarting(container));
    }

    private static boolean isAgentStarting(MSSQLServerContainer<?> container) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                container.getJdbcUrl(),
                                container.getUsername(),
                                container.getPassword());
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(AGENT_STARTING_QUERY)) {
            resultSet.next();
            return resultSet.getInt(1) != 0;
        }
    }
}
