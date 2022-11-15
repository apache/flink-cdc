/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.postgres.source.utils;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A CustomPostgresSchema similar to PostgresSchema with customization. */
public class CustomPostgresSchema {

    // cache the schema for each table
    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>();
    private final PostgresConnection jdbcConnection;
    private final PostgresConnectorConfig dbzConfig;

    public CustomPostgresSchema(
            PostgresConnection jdbcConnection, PostgresSourceConfig sourceConfig) {
        this.jdbcConnection = jdbcConnection;
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
    }

    public TableChange getTableSchema(TableId tableId) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                readTableSchema(tableId);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    private TableChange readTableSchema(TableId tableId) throws SQLException {

        final PostgresOffsetContext offsetContext =
                PostgresOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());

        // set the events to populate proper sourceInfo into offsetContext
        offsetContext.event(tableId, Instant.now());

        Tables tables = new Tables();
        try {
            jdbcConnection.readSchema(
                    tables,
                    dbzConfig.databaseName(),
                    tableId.schema(),
                    dbzConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read schema", e);
        }

        Table table = Objects.requireNonNull(tables.forTable(tableId));

        // TODO: check whether we always set isFromSnapshot = true
        SchemaChangeEvent schemaChangeEvent =
                SchemaChangeEvent.ofCreate(
                        partition,
                        offsetContext,
                        dbzConfig.databaseName(),
                        tableId.schema(),
                        null,
                        table,
                        true);

        for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
            this.schemasByTableId.put(tableId, tableChange);
        }
        return this.schemasByTableId.get(tableId);
    }
}
