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

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestBase;
import org.apache.flink.table.utils.LegacyRowResource;

import org.junit.ClassRule;

/** Basic class for testing OceanBase source. */
public abstract class LegacyOceanBaseTestBase extends OceanBaseTestBase {

    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    protected final String compatibleMode;
    protected final String username;
    protected final String password;
    protected final String hostname;
    protected final int port;
    protected final String logProxyHost;
    protected final int logProxyPort;
    protected final String tenant;

    public LegacyOceanBaseTestBase(
            String compatibleMode,
            String username,
            String password,
            String hostname,
            int port,
            String logProxyHost,
            int logProxyPort,
            String tenant) {
        this.compatibleMode = compatibleMode;
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.tenant = tenant;
    }

    @Override
    protected String getCompatibleMode() {
        return compatibleMode;
    }

    protected String commonOptionsString() {
        return String.format(
                " 'connector' = 'oceanbase-cdc', "
                        + " 'username' = '%s', "
                        + " 'password' = '%s', "
                        + " 'hostname' = '%s', "
                        + " 'port' = '%s', "
                        + " 'compatible-mode' = '%s', "
                        + " 'scan.incremental.snapshot.enabled' = 'false'",
                username, password, hostname, port, compatibleMode);
    }

    protected String logProxyOptionsString() {
        return String.format(
                " 'working-mode' = 'memory',"
                        + " 'tenant-name' = '%s',"
                        + " 'logproxy.host' = '%s',"
                        + " 'logproxy.port' = '%s'",
                tenant, logProxyHost, logProxyPort);
    }

    protected String initialOptionsString() {
        return " 'scan.startup.mode' = 'initial', "
                + commonOptionsString()
                + ", "
                + logProxyOptionsString();
    }

    protected String snapshotOptionsString() {
        return " 'scan.startup.mode' = 'snapshot', " + commonOptionsString();
    }
}
