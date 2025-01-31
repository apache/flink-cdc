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

package org.apache.flink.cdc.connectors.oceanbase.source.offset;

import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** OceanBase partition. */
public class OceanBasePartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public OceanBasePartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OceanBasePartition other = (OceanBasePartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "OceanBasePartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<OceanBasePartition> {
        private final OceanBaseConnectorConfig connectorConfig;

        Provider(OceanBaseConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<OceanBasePartition> getPartitions() {
            return Collections.singleton(new OceanBasePartition(connectorConfig.getLogicalName()));
        }
    }
}
