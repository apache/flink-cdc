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

package com.alibaba.ververica.cdc.connectors.oracle.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.connectors.oracle.OracleSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle binlog from a logical
 * description.
 */
public class OracleTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String tableName;
    private final String schemaName;
    private final Properties dbzProperties;
    private final StartupOptions startupOptions;

    public OracleTableSource(
            TableSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String schemaName,
            String username,
            String password,
            Properties dbzProperties,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.schemaName = checkNotNull(schemaName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(physicalSchema.toRowDataType());

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(rowType)
                        .setResultTypeInfo(typeInfo)
                        .build();
        OracleSource.Builder<RowData> builder =
                OracleSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .tableList(schemaName + "." + tableName)
                        .schemaList(schemaName)
                        .username(username)
                        .password(password)
                        .serverTimeZone(serverTimeZone.toString())
                        .debeziumProperties(dbzProperties)
                        .startupOptions(startupOptions)
                        .deserializer(deserializer);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new OracleTableSource(
                physicalSchema,
                port,
                hostname,
                database,
                tableName,
                schemaName,
                username,
                password,
                dbzProperties,
                startupOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OracleTableSource that = (OracleTableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(startupOptions, that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                username,
                password,
                tableName,
                schemaName,
                dbzProperties,
                startupOptions);
    }

    @Override
    public String asSummaryString() {
        return "Oracle-CDC";
    }
}
