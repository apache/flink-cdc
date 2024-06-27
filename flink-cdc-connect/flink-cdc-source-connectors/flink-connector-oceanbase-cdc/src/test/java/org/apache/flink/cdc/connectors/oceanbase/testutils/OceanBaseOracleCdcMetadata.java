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

/** OceanBase CDC Oracle mode metadata. */
public class OceanBaseOracleCdcMetadata implements OceanBaseCdcMetadata {

    @Override
    public String getCompatibleMode() {
        return "oracle";
    }

    @Override
    public String getHostname() {
        return System.getenv("host");
    }

    @Override
    public int getPort() {
        return Integer.parseInt(System.getenv("port"));
    }

    @Override
    public String getUsername() {
        return System.getenv("username");
    }

    @Override
    public String getPassword() {
        return System.getenv("password");
    }

    @Override
    public String getDatabase() {
        return System.getenv("schema");
    }

    @Override
    public String getDriverClass() {
        return "com.oceanbase.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:oceanbase://" + getHostname() + ":" + getPort() + "/" + getDatabase();
    }

    @Override
    public String getTenantName() {
        return System.getenv("tenant");
    }

    @Override
    public String getLogProxyHost() {
        return System.getenv("log_proxy_host");
    }

    @Override
    public int getLogProxyPort() {
        return Integer.parseInt(System.getenv("log_proxy_port"));
    }

    @Override
    public String getConfigUrl() {
        return System.getenv("config_url");
    }
}
