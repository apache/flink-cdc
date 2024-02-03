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

package com.ververica.cdc.runtime.operators.route;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.ververica.cdc.common.utils.Preconditions.checkState;

/** A map function that applies user-defined routing logics. */
public class RouteFunction extends RichMapFunction<Event, Event> {
    private final List<Tuple3<String, String, TableId>> routingRules;
    private transient List<Tuple3<Selectors, String, TableId>> routes;

    private static final String DEFAULT_TABLE_NAME_REPLACE_SYMBOL = "<>";

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link RouteFunction}. */
    public static class Builder {
        private final List<Tuple3<String, String, TableId>> routingRules = new ArrayList<>();

        public Builder addRoute(String tableInclusions, String replaceSymbol, TableId replaceBy) {
            String tableNameReplaceSymbol =
                    replaceSymbol == null || replaceSymbol.isEmpty()
                            ? DEFAULT_TABLE_NAME_REPLACE_SYMBOL
                            : replaceSymbol;
            routingRules.add(Tuple3.of(tableInclusions, tableNameReplaceSymbol, replaceBy));
            return this;
        }

        public Builder addRoute(String tableInclusions, TableId replaceBy) {
            return addRoute(tableInclusions, null, replaceBy);
        }

        public RouteFunction build() {
            return new RouteFunction(routingRules);
        }
    }

    private RouteFunction(List<Tuple3<String, String, TableId>> routingRules) {
        this.routingRules = routingRules;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        routes =
                routingRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    String tableNameReplaceSymbol = tuple2.f1;
                                    TableId replaceBy = tuple2.f2;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple3<>(
                                            selectors, tableNameReplaceSymbol, replaceBy);
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public Event map(Event event) throws Exception {
        checkState(
                event instanceof ChangeEvent,
                String.format(
                        "The input event of the route is not a ChangeEvent but with type \"%s\"",
                        event.getClass().getCanonicalName()));
        ChangeEvent changeEvent = (ChangeEvent) event;
        TableId tableId = changeEvent.tableId();

        for (Tuple3<Selectors, String, TableId> route : routes) {
            Selectors selectors = route.f0;
            String tableNameReplaceSymbol = route.f1;
            TableId replaceBy = route.f2;
            if (selectors.isMatch(tableId)) {
                // Add a rule that when configuring tableNameReplaceSymbol in tablename,
                // the namespace name and schemaName name needs to be changed
                // the tableNameReplaceSymbol needs to be replaced by the table name of the event
                if (replaceBy.getTableName().contains(tableNameReplaceSymbol)) {
                    replaceBy =
                            TableId.parse(
                                    replaceBy.getNamespace(),
                                    replaceBy.getSchemaName(),
                                    replaceBy
                                            .getTableName()
                                            .replace(
                                                    tableNameReplaceSymbol,
                                                    tableId.getTableName()));
                }
                return recreateChangeEvent(changeEvent, replaceBy);
            }
        }
        return event;
    }

    private ChangeEvent recreateChangeEvent(ChangeEvent event, TableId tableId) {
        if (event instanceof DataChangeEvent) {
            return recreateDataChangeEvent(((DataChangeEvent) event), tableId);
        }
        if (event instanceof SchemaChangeEvent) {
            return recreateSchemaChangeEvent(((SchemaChangeEvent) event), tableId);
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported change event with type \"%s\"",
                        event.getClass().getCanonicalName()));
    }

    private DataChangeEvent recreateDataChangeEvent(
            DataChangeEvent dataChangeEvent, TableId tableId) {
        switch (dataChangeEvent.op()) {
            case INSERT:
                return DataChangeEvent.insertEvent(
                        tableId, dataChangeEvent.after(), dataChangeEvent.meta());
            case UPDATE:
                return DataChangeEvent.updateEvent(
                        tableId,
                        dataChangeEvent.before(),
                        dataChangeEvent.after(),
                        dataChangeEvent.meta());
            case REPLACE:
                return DataChangeEvent.replaceEvent(
                        tableId, dataChangeEvent.after(), dataChangeEvent.meta());
            case DELETE:
                return DataChangeEvent.deleteEvent(
                        tableId, dataChangeEvent.before(), dataChangeEvent.meta());
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported operation type \"%s\" in data change event",
                                dataChangeEvent.op()));
        }
    }

    private SchemaChangeEvent recreateSchemaChangeEvent(
            SchemaChangeEvent schemaChangeEvent, TableId tableId) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
            return new CreateTableEvent(tableId, createTableEvent.getSchema());
        }
        if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
            AlterColumnTypeEvent alterColumnTypeEvent = (AlterColumnTypeEvent) schemaChangeEvent;
            return new AlterColumnTypeEvent(tableId, alterColumnTypeEvent.getTypeMapping());
        }
        if (schemaChangeEvent instanceof RenameColumnEvent) {
            RenameColumnEvent renameColumnEvent = (RenameColumnEvent) schemaChangeEvent;
            return new RenameColumnEvent(tableId, renameColumnEvent.getNameMapping());
        }
        if (schemaChangeEvent instanceof DropColumnEvent) {
            DropColumnEvent dropColumnEvent = (DropColumnEvent) schemaChangeEvent;
            return new DropColumnEvent(tableId, dropColumnEvent.getDroppedColumnNames());
        }
        if (schemaChangeEvent instanceof AddColumnEvent) {
            AddColumnEvent addColumnEvent = (AddColumnEvent) schemaChangeEvent;
            return new AddColumnEvent(tableId, addColumnEvent.getAddedColumns());
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported schema change event with type \"%s\"",
                        schemaChangeEvent.getClass().getCanonicalName()));
    }
}
