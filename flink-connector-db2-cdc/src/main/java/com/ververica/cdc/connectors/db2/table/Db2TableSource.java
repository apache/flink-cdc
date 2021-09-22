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

package com.ververica.cdc.connectors.db2.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.db2.Db2Source;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

/** TableSource for DB2 CDC connector. */
public class Db2TableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String schemaName;
    private final String tableName;
    private final String username;
    private final String password;
    private final ZoneId serverTimeZone;
    private final StartupOptions startupOptions;
    private final Properties dbzProperties;

    public Db2TableSource(
            TableSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String schemaName,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = hostname;
        this.database = database;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(physicalSchema.toRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer =
                new RowDataDebeziumDeserializeSchema(
                        rowType, typeInfo, ((rowData, rowKind) -> {}), serverTimeZone);
        Db2Source.Builder<RowData> builder =
                Db2Source.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .tableList(schemaName + "." + tableName)
                        .username(username)
                        .password(password)
                        .debeziumProperties(dbzProperties)
                        .deserializer(deserializer)
                        .startupOptions(startupOptions);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new Db2TableSource(
                physicalSchema,
                port,
                hostname,
                database,
                schemaName,
                tableName,
                username,
                password,
                serverTimeZone,
                dbzProperties,
                startupOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Db2TableSource)) {
            return false;
        }
        Db2TableSource that = (Db2TableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                schemaName,
                tableName,
                username,
                password,
                serverTimeZone,
                dbzProperties);
    }

    @Override
    public String asSummaryString() {
        return "DB2-CDC";
    }
}
