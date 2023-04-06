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

package com.ververica.cdc.connectors.table.json;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.table.deserializa.CdcJsonRowDataDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a PostgreSQL source from a logical
 * description.
 */
public class CdcJsonSourceTable implements ScanTableSource {

    private final ResolvedSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final List<String> schemaList;
    private final List<String> tableList;
    private final String username;
    private final String password;
    private final Properties dbzProperties;
    private final String sourceType;
    private final Map<String, String> extraProperties;
    private final List<String> databaseList;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Data type that describes the final output of the source.
     */
    protected DataType producedDataType;

    public CdcJsonSourceTable(ResolvedSchema physicalSchema, int port, String hostname, List<String> databaseList, List<String> schemaList, List<String> tableList, String username, String password,
                              String sourceType, Map<String, String> extraProperties, Properties debeziumProperties) {

        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.databaseList = checkNotNull(databaseList);
        this.schemaList = checkNotNull(schemaList);
        this.tableList = checkNotNull(tableList);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.sourceType = checkNotNull(sourceType);
        this.extraProperties = new HashMap<>(extraProperties);
        extraProperties.keySet().forEach(key -> {
            if (key.startsWith("debezium.")) {
                this.extraProperties.remove(key);
            }
        });
        this.dbzProperties = debeziumProperties;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (sourceType.equals("postgres")) {
            return getPostgreCdc();
        } else if (sourceType.equals("mysql")) {
            return getMysqlCdc();
        } else if (sourceType.equals("oracle")) {
            return getOracleCdc();
        } else if (sourceType.equals("sqlserver")) {
            return getSqlserverCdc();
        }
        throw new RuntimeException("不支持此种类型的cdc:" + sourceType);
    }

    private ScanRuntimeProvider getPostgreCdc() {
        SourceFunction<RowData> sourceFunction = PostgreSQLSource.<RowData>builder()
                .hostname(hostname)
                .port(port)
                .database(databaseList.get(0)) // monitor postgres database
                .schemaList(schemaList.toArray(new String[schemaList.size()]))  // monitor inventory schema
                .tableList(tableList.toArray(new String[tableList.size()])) // monitor products table
                .username(username)
                .password(password)
                .deserializer(new CdcJsonRowDataDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
                .debeziumProperties(dbzProperties)
                .build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    private ScanRuntimeProvider getMysqlCdc() {
        String scanStartMode = extraProperties.getOrDefault("scan.startup.mode", "initial");
        StartupOptions scanStartOptions = StartupOptions.initial();
        if (scanStartMode.equals("latest-offset")) {
            scanStartOptions = StartupOptions.latest();
        }

        MySqlSource<RowData> sourceFunction = MySqlSource.<RowData>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(databaseList.toArray(new String[databaseList.size()])) // monitor postgres database
                .tableList(tableList.toArray(new String[tableList.size()])) // monitor products table
                .username(username)
                .password(password)
                .serverTimeZone(this.extraProperties.getOrDefault("server-time-zone", "Asia/Shanghai"))
                .includeSchemaChanges(Boolean.valueOf(extraProperties.getOrDefault("include-schema-changes", "false")))
                .scanNewlyAddedTableEnabled(Boolean.valueOf(extraProperties.getOrDefault("scan-new-table", "false")))
                .deserializer(new CdcJsonRowDataDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
                .startupOptions(scanStartOptions)
                .debeziumProperties(dbzProperties)
                .build();

        return SourceProvider.of(sourceFunction);
    }

    private ScanRuntimeProvider getOracleCdc() {
        DebeziumSourceFunction<RowData> sourceFunction = OracleSource.<RowData>builder()
                .hostname(hostname)
                .port(port)
                .database(databaseList.get(0)) // monitor postgres database
                .tableList(tableList.toArray(new String[tableList.size()])) // monitor products table
                .username(username)
                .password(password)
                .deserializer(new CdcJsonRowDataDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
                .debeziumProperties(dbzProperties)
                .build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    private ScanRuntimeProvider getSqlserverCdc() {
        DebeziumSourceFunction<RowData> sourceFunction = SqlServerSource.<RowData>builder()
                .hostname(hostname)
                .port(port)
                .database(databaseList.get(0)) // monitor postgres database
                .tableList(tableList.toArray(new String[tableList.size()])) // monitor products table
                .username(username)
                .password(password)
                .deserializer(new CdcJsonRowDataDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
                .debeziumProperties(dbzProperties)
                .build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        CdcJsonSourceTable source =
                new CdcJsonSourceTable(
                        physicalSchema,
                        port,
                        hostname,
                        databaseList,
                        schemaList,
                        tableList,
                        username,
                        password,
                        sourceType,
                        extraProperties,
                        dbzProperties);
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdcJsonSourceTable that = (CdcJsonSourceTable) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(databaseList, that.databaseList)
                && Objects.equals(schemaList, that.schemaList)
                && Objects.equals(tableList, that.tableList)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(producedDataType, that.producedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                databaseList,
                schemaList,
                tableList,
                username,
                password,
                dbzProperties,
                producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "PostgreSQL-CDC";
    }

}
