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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Utilities for handling {@link org.apache.flink.cdc.common.event.ChangeEvent}s. */
public class ChangeEventUtils {
    public static DataChangeEvent recreateDataChangeEvent(
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

    public static SchemaChangeEvent recreateSchemaChangeEvent(
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

    public static Set<SchemaChangeEventType> resolveSchemaEvolutionOptions(
            List<String> includedSchemaEvolutionTypes, List<String> excludedSchemaEvolutionTypes) {
        List<SchemaChangeEventType> resultTypes = new ArrayList<>();

        if (includedSchemaEvolutionTypes.isEmpty()) {
            resultTypes.addAll(Arrays.asList(SchemaChangeEventTypeFamily.ALL));
        } else {
            for (String includeTag : includedSchemaEvolutionTypes) {
                resultTypes.addAll(resolveSchemaEvolutionTag(includeTag));
            }
        }

        for (String excludeTag : excludedSchemaEvolutionTypes) {
            resultTypes.removeAll(resolveSchemaEvolutionTag(excludeTag));
        }

        return new HashSet<>(resultTypes);
    }

    @VisibleForTesting
    public static List<SchemaChangeEventType> resolveSchemaEvolutionTag(String tag) {
        List<SchemaChangeEventType> types =
                new ArrayList<>(Arrays.asList(SchemaChangeEventTypeFamily.ofTag(tag)));
        if (types.isEmpty()) {
            // It's a specified tag
            SchemaChangeEventType type = SchemaChangeEventType.ofTag(tag);
            if (type != null) {
                types.add(type);
            }
        }
        return types;
    }
}
