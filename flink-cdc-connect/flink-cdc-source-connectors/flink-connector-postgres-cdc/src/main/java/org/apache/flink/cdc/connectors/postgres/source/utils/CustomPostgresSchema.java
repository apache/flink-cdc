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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A CustomPostgresSchema similar to PostgresSchema with customization.
 *
 * <p>This class caches table schemas and creates PostgresSchema instances on-demand using the
 * provided connection. Each query method accepts a connection parameter to ensure fresh connections
 * are used.
 */
public class CustomPostgresSchema {

    // cache the schema for each table
    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>(32);
    private final PostgresConnectorConfig dbzConfig;

    @Nullable private final PostgresPartitionRouter partitionRouter;

    /**
     * Creates a CustomPostgresSchema.
     *
     * @param sourceConfig the source configuration
     * @param partitionRouter the partition router (nullable)
     */
    public CustomPostgresSchema(
            PostgresSourceConfig sourceConfig, @Nullable PostgresPartitionRouter partitionRouter) {
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
        this.partitionRouter = partitionRouter;
    }

    /**
     * Gets the table schema for a single table.
     *
     * @param jdbcConnection the connection to use for querying
     * @param tableId the table ID
     * @return the table change containing schema information
     */
    public TableChange getTableSchema(PostgresConnection jdbcConnection, TableId tableId) {
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                readTableSchema(jdbcConnection, Collections.singletonList(tableId));
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    /**
     * Gets the table schemas for multiple tables.
     *
     * @param jdbcConnection the connection to use for querying
     * @param tableIds the list of table IDs
     * @return map of table ID to table change
     */
    public Map<TableId, TableChange> getTableSchema(
            PostgresConnection jdbcConnection, List<TableId> tableIds) {
        Map<TableId, TableChange> tableChanges = new HashMap<>(32);

        List<TableId> uncachedTableIds = new ArrayList<>();
        for (TableId tableId : tableIds) {
            if (schemasByTableId.containsKey(tableId)) {
                tableChanges.put(tableId, schemasByTableId.get(tableId));
            } else {
                uncachedTableIds.add(tableId);
            }
        }

        if (!uncachedTableIds.isEmpty()) {
            try {
                readTableSchema(jdbcConnection, uncachedTableIds);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
            for (TableId tableId : uncachedTableIds) {
                if (schemasByTableId.containsKey(tableId)) {
                    tableChanges.put(tableId, schemasByTableId.get(tableId));
                } else {
                    throw new FlinkRuntimeException(
                            String.format("Failed to read table schema of table %s", tableId));
                }
            }
        }
        return tableChanges;
    }

    private void readTableSchema(PostgresConnection jdbcConnection, List<TableId> tableIds)
            throws SQLException {
        // Create PostgresSchema using the provided connection
        PostgresSchema postgresSchema = createPostgresSchema(jdbcConnection);

        final PostgresOffsetContext offsetContext =
                PostgresOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());

        for (TableId tableId : tableIds) {
            Table table = postgresSchema.tableFor(tableId);
            if (table == null) {
                throw new FlinkRuntimeException(
                        String.format("Failed to read table schema for table %s", tableId));
            }

            offsetContext.event(tableId, Instant.now());

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
        }
    }

    /**
     * Creates a PostgresSchema instance using the provided connection.
     *
     * @param jdbcConnection the connection to use
     * @return a new PostgresSchema instance
     */
    private PostgresSchema createPostgresSchema(PostgresConnection jdbcConnection)
            throws SQLException {
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);
        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                PostgresObjectUtils.newPostgresValueConverterBuilder(dbzConfig);

        return new PostgresPartitionRoutingSchema(
                jdbcConnection,
                dbzConfig,
                jdbcConnection.getTypeRegistry(),
                topicSelector,
                valueConverterBuilder.build(jdbcConnection.getTypeRegistry()),
                partitionRouter);
    }
}
