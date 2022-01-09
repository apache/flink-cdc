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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a TiDB binlog from a logical
 * description.
 */
public class TiDBTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final String hostname;
    private final String database;
    private final String tableName;
    private final String username;
    private final String password;
    private final StartupOptions startupOptions;
    private final Map<String, String> options;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    public TiDBTableSource(
            TableSchema physicalSchema,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            StartupOptions startupOptions,
            Map<String, String> options) {
        this.physicalSchema = physicalSchema;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                // TODO TiKV cdc client doesn't return old value in PUT event
                // .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final TiConfiguration tiConf = TDBSourceOptions.getTiConfiguration(options);
        try (final TiSession session = TiSession.create(tiConf)) {
            final TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);

            TypeInformation<RowData> typeInfo =
                    scanContext.createTypeInformation(physicalSchema.toRowDataType());
            RowDataTiKVSnapshotEventDeserializationSchema snapshotEventDeserializationSchema =
                    new RowDataTiKVSnapshotEventDeserializationSchema(typeInfo, tableInfo);
            RowDataTiKVChangeEventDeserializationSchema changeEventDeserializationSchema =
                    new RowDataTiKVChangeEventDeserializationSchema(typeInfo, tableInfo);

            TiDBSource.Builder<RowData> builder =
                    TiDBSource.<RowData>builder()
                            .hostname(hostname)
                            .database(database)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .startupOptions(startupOptions)
                            .tiConf(tiConf)
                            .tiTableInfo(tableInfo)
                            .snapshotEventDeserializer(snapshotEventDeserializationSchema)
                            .changeEventDeserializer(changeEventDeserializationSchema);
            return SourceFunctionProvider.of(builder.build(), false);
        } catch (final Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public DynamicTableSource copy() {
        TiDBTableSource source =
                new TiDBTableSource(
                        physicalSchema,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        startupOptions,
                        options);
        source.producedDataType = producedDataType;
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
        TiDBTableSource that = (TiDBTableSource) o;
        return Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                hostname,
                database,
                username,
                password,
                tableName,
                startupOptions,
                producedDataType,
                options);
    }

    @Override
    public String asSummaryString() {
        return "TiDB-CDC";
    }
}
