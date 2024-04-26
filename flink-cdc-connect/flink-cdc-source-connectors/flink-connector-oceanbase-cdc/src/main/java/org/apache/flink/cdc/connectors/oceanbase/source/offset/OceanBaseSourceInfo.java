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

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** OceanBase source info. */
public class OceanBaseSourceInfo extends BaseSourceInfo {

    public static final String TENANT_KEY = "tenant";
    public static final String TRANSACTION_ID_KEY = "transaction_id";

    private final String tenant;

    private Instant sourceTime;
    private Set<TableId> tableIds;
    private String transactionId;

    public OceanBaseSourceInfo(OceanBaseConnectorConfig config, String tenant) {
        super(config);
        this.tenant = tenant;
    }

    public String tenant() {
        return tenant;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public void beginTransaction(String transactionId) {
        this.transactionId = transactionId;
    }

    public void commitTransaction() {
        this.transactionId = null;
    }

    public String transactionId() {
        return transactionId;
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    @Override
    protected String database() {
        return (tableIds != null) ? tableIds.iterator().next().catalog() : null;
    }

    public String tableSchema() {
        return (tableIds == null || tableIds.isEmpty())
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::schema)
                        .filter(Objects::nonNull)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    public String table() {
        return (tableIds == null || tableIds.isEmpty())
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }
}
