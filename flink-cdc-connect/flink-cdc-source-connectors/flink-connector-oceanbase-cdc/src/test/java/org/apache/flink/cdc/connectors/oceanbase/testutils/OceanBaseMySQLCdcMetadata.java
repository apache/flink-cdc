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

package org.apache.flink.cdc.connectors.oceanbase.testutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** OceanBase CDC MySQL mode metadata. */
public class OceanBaseMySQLCdcMetadata implements OceanBaseCdcMetadata {

    private final OceanBaseContainer obServerContainer;
    private final LogProxyContainer logProxyContainer;

    private String rsList;

    public OceanBaseMySQLCdcMetadata(
            OceanBaseContainer obServerContainer, LogProxyContainer logProxyContainer) {
        this.obServerContainer = obServerContainer;
        this.logProxyContainer = logProxyContainer;
    }

    @Override
    public String getCompatibleMode() {
        return "mysql";
    }

    @Override
    public String getHostname() {
        return obServerContainer.getHost();
    }

    @Override
    public int getPort() {
        return obServerContainer.getDatabasePort();
    }

    @Override
    public String getUsername() {
        return obServerContainer.getUsername();
    }

    @Override
    public String getPassword() {
        return obServerContainer.getPassword();
    }

    @Override
    public String getDriverClass() {
        return obServerContainer.getDriverClassName();
    }

    @Override
    public String getDatabase() {
        return obServerContainer.getDatabaseName();
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://" + getHostname() + ":" + getPort() + "/?useSSL=false";
    }

    @Override
    public String getTenantName() {
        return obServerContainer.getTenantName();
    }

    @Override
    public String getLogProxyHost() {
        return logProxyContainer.getHost();
    }

    @Override
    public int getLogProxyPort() {
        return logProxyContainer.getPort();
    }

    @Override
    public String getRsList() {
        if (rsList == null) {
            try (Connection connection =
                            DriverManager.getConnection(
                                    getJdbcUrl(), getUsername(), getPassword());
                    Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SHOW PARAMETERS LIKE 'rootservice_list'");
                rsList = rs.next() ? rs.getString("VALUE") : null;
            } catch (SQLException e) {
                throw new RuntimeException("Failed to query rs list", e);
            }
            if (rsList == null) {
                throw new RuntimeException("Got empty rs list");
            }
        }
        return rsList;
    }
}
