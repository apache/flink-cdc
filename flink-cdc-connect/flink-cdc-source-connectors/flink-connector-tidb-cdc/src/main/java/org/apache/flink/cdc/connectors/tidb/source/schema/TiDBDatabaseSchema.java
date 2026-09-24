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

package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBDefaultValueConverter;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;

import io.debezium.connector.tidb.TiDBAntlrDdlParser;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** OceanBase database schema. */
public class TiDBDatabaseSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBDatabaseSchema.class);
    private final Set<String> ignoredQueryStatements =
            Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final RelationalTableFilters filters;
    private final DdlParser ddlParser;
    private final DdlChanges ddlChanges;

    public TiDBDatabaseSchema(
            TiDBConnectorConfig config,
            TiDBValueConverters tiDBValueConverters,
            TopicSelector<TableId> topicSelector,
            boolean tableIdCaseInsensitive) {
        super(
                config,
                topicSelector,
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        tiDBValueConverters,
                        new TiDBDefaultValueConverter(tiDBValueConverters),
                        config.schemaNameAdjustmentMode().createAdjuster(),
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getSanitizeFieldNames(),
                        false),
                tableIdCaseInsensitive,
                config.getKeyMapper());

        // todo  change
        this.ddlParser =
                new TiDBAntlrDdlParser(
                        true,
                        false,
                        config.isSchemaCommentsHistoryEnabled(),
                        tiDBValueConverters,
                        getTableFilter());
        filters = config.getTableFilters();
        this.ddlChanges = this.ddlParser.getDdlChanges();
    }

    public TiDBDatabaseSchema refresh(
            TiDBConnection connection, TiDBConnectorConfig config, boolean printReplicaIdentityInfo)
            throws SQLException {
        // read all the information from the DB
        //        connection.readSchema(tables(), null, null, getTableFilter(), null, true);
        //        LOGGER.info("TiDBDatabaseSchema refresh **********");
        connection.readTiDBSchema(config, this, tables(), null, null, getTableFilter(), null, true);

        //    if (printReplicaIdentityInfo) {
        //      // print out all the replica identity info
        //      tableIds().forEach(tableId -> printReplicaIdentityInfo(connection, tableId));
        //    }
        // and then refresh the schemas
        refreshSchemas();
        //    if (readToastableColumns) {
        //      tableIds().forEach(tableId -> refreshToastableColumnsMap(connection, tableId));
        //    }
        return this;
    }

    protected void refreshSchemas() {
        clearSchemas();
        // Create TableSchema instances for any existing table ...
        tableIds().forEach(this::refreshSchema);
    }

    @Override
    protected void refreshSchema(TableId id) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);
        buildAndRegisterSchema(table);
    }

    public List<SchemaChangeEvent> parseSnapshotDdl(
            TiDBPartition partition,
            String ddlStatements,
            String databaseName,
            EventOffsetContext offset,
            Instant sourceTime) {
        LOGGER.debug("Processing snapshot DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, true);
    }

    private List<SchemaChangeEvent> parseDdl(
            TiDBPartition partition,
            String ddlStatements,
            String databaseName,
            EventOffsetContext offset,
            Instant sourceTime,
            boolean snapshot) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);

        if (ignoredQueryStatements.contains(ddlStatements)) {
            return schemaChangeEvents;
        }

        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, tables());
        } catch (ParsingException | MultipleParsingExceptions e) {
            throw e;
        }
        if (!ddlChanges.isEmpty()) {
            ddlChanges.getEventsByDatabase(
                    (String dbName, List<DdlParserListener.Event> events) -> {
                        final String sanitizedDbName = (dbName == null) ? "" : dbName;
                        if (acceptableDatabase(dbName)) {
                            final Set<TableId> tableIds = new HashSet<>();
                            events.forEach(
                                    event -> {
                                        final TableId tableId = getTableId(event);
                                        if (tableId != null) {
                                            tableIds.add(tableId);
                                        }
                                    });
                            events.forEach(
                                    event -> {
                                        final TableId tableId = getTableId(event);
                                        offset.tableEvent(dbName, tableIds, sourceTime);
                                        // For SET with multiple parameters
                                        if (event instanceof DdlParserListener.TableCreatedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.CREATE,
                                                    snapshot);
                                        } else if (event
                                                        instanceof
                                                        DdlParserListener.TableAlteredEvent
                                                || event
                                                        instanceof
                                                        DdlParserListener.TableIndexCreatedEvent
                                                || event
                                                        instanceof
                                                        DdlParserListener.TableIndexDroppedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.ALTER,
                                                    snapshot);
                                        } else if (event
                                                instanceof DdlParserListener.TableDroppedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.DROP,
                                                    snapshot);
                                        } else if (event
                                                instanceof DdlParserListener.SetVariableEvent) {
                                            // SET statement with multiple variable emits event for
                                            // each variable. We want to emit only
                                            // one change event
                                            final DdlParserListener.SetVariableEvent varEvent =
                                                    (DdlParserListener.SetVariableEvent) event;
                                            if (varEvent.order() == 0) {
                                                emitChangeEvent(
                                                        partition,
                                                        offset,
                                                        schemaChangeEvents,
                                                        sanitizedDbName,
                                                        event,
                                                        tableId,
                                                        SchemaChangeEvent.SchemaChangeEventType
                                                                .DATABASE,
                                                        snapshot);
                                            }
                                        } else {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType
                                                            .DATABASE,
                                                    snapshot);
                                        }
                                    });
                        }
                    });
        } else {
            offset.databaseEvent(databaseName, sourceTime);
            schemaChangeEvents.add(
                    SchemaChangeEvent.ofDatabase(
                            partition, offset, databaseName, ddlStatements, snapshot));
        }
        return schemaChangeEvents;
    }

    private boolean acceptableDatabase(final String databaseName) {
        return filters.databaseFilter().test(databaseName)
                || databaseName == null
                || databaseName.isEmpty();
    }

    private TableId getTableId(DdlParserListener.Event event) {
        if (event instanceof DdlParserListener.TableEvent) {
            return ((DdlParserListener.TableEvent) event).tableId();
        } else if (event instanceof DdlParserListener.TableIndexEvent) {
            return ((DdlParserListener.TableIndexEvent) event).tableId();
        }
        return null;
    }

    private void emitChangeEvent(
            TiDBPartition partition,
            EventOffsetContext offset,
            List<SchemaChangeEvent> schemaChangeEvents,
            final String sanitizedDbName,
            DdlParserListener.Event event,
            TableId tableId,
            SchemaChangeEvent.SchemaChangeEventType type,
            boolean snapshot) {
        SchemaChangeEvent schemaChangeEvent;
        if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER)
                && event instanceof DdlParserListener.TableAlteredEvent
                && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
            schemaChangeEvent =
                    SchemaChangeEvent.ofRename(
                            partition,
                            offset,
                            sanitizedDbName,
                            null,
                            event.statement(),
                            tableId != null ? tableFor(tableId) : null,
                            ((DdlParserListener.TableAlteredEvent) event).previousTableId());
        } else {
            schemaChangeEvent =
                    SchemaChangeEvent.of(
                            type,
                            partition,
                            offset,
                            sanitizedDbName,
                            null,
                            event.statement(),
                            tableId != null ? tableFor(tableId) : null,
                            snapshot);
        }
        schemaChangeEvents.add(schemaChangeEvent);
    }
}
