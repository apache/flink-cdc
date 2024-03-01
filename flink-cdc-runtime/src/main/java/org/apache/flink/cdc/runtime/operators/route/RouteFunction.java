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

package org.apache.flink.cdc.runtime.operators.route;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.Preconditions.checkState;

/** A map function that applies user-defined routing logics. */
public class RouteFunction extends RichMapFunction<Event, Event> {
    private final List<Tuple2<String, TableId>> routingRules;
    private transient List<Tuple2<Selectors, TableId>> routes;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link RouteFunction}. */
    public static class Builder {
        private final List<Tuple2<String, TableId>> routingRules = new ArrayList<>();

        public Builder addRoute(String tableInclusions, TableId replaceBy) {
            routingRules.add(Tuple2.of(tableInclusions, replaceBy));
            return this;
        }

        public RouteFunction build() {
            return new RouteFunction(routingRules);
        }
    }

    private RouteFunction(List<Tuple2<String, TableId>> routingRules) {
        this.routingRules = routingRules;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        routes =
                routingRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    TableId replaceBy = tuple2.f1;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(selectors, replaceBy);
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

        for (Tuple2<Selectors, TableId> route : routes) {
            Selectors selectors = route.f0;
            TableId replaceBy = route.f1;
            if (selectors.isMatch(tableId)) {
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
