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

package com.ververica.cdc.connectors.values.source;

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A helper class for {@link ValuesDataSource} to build events of each split. */
public class ValuesDataSourceHelper {

    /**
     * Different situations for creating sourceEvents, {@link
     * ValuesDataSourceOptions#SOURCE_EVENT_TYPE}.
     */
    public enum SourceEventType {
        SINGLE_SPLIT_SINGLE_TABLE,
        SINGLE_SPLIT_MULTI_TABLES,
        CUSTOM_SOURCE_EVENTS
    }

    private static final TableId table1 = TableId.tableId("default", "default", "table1");

    private static final TableId table2 = TableId.tableId("default", "default", "table2");

    /**
     * create events of {@link DataChangeEvent} and {@link SchemaChangeEvent} for {@link
     * ValuesDataSource}.
     */
    private static List<List<Event>> sourceEvents;

    public static List<List<Event>> getSourceEvents() {
        if (sourceEvents == null) {
            throw new IllegalArgumentException(
                    "sourceEvents should be set by `setSourceEvents` method.");
        }
        return sourceEvents;
    }

    /** set sourceEvents using custom events. */
    public static void setSourceEvents(List<List<Event>> customSourceEvents) {
        sourceEvents = customSourceEvents;
    }

    /** set sourceEvents using predefined events. */
    public static void setSourceEvents(SourceEventType eventType) {
        switch (eventType) {
            case SINGLE_SPLIT_SINGLE_TABLE:
                {
                    sourceEvents = singleSplitSingleTable();
                    break;
                }

            case SINGLE_SPLIT_MULTI_TABLES:
                {
                    sourceEvents = singleSplitMultiTables();
                    break;
                }
            case CUSTOM_SOURCE_EVENTS:
                {
                    break;
                }
            default:
                throw new IllegalArgumentException(eventType + " is not supported");
        }
    }

    private static List<List<Event>> singleSplitSingleTable() {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        split1.add(createTableEvent);

        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1")));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("3"),
                                GenericStringData.fromString("3")));
        split1.add(insertEvent3);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        split1.add(addColumnEvent);

        // rename column
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        split1.add(renameColumnEvent);

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", DataTypes.STRING())));
        split1.add(dropColumnEvent);

        // delete
        split1.add(
                DataChangeEvent.deleteEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1"))));

        // update
        split1.add(
                DataChangeEvent.updateEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("")),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("x"))));

        eventOfSplits.add(split1);
        return eventOfSplits;
    }

    private static List<List<Event>> singleSplitMultiTables() {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        split1.add(createTableEvent);
        CreateTableEvent createTableEvent2 = new CreateTableEvent(table2, schema);
        split1.add(createTableEvent2);

        // insert into table1
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1")));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("3"),
                                GenericStringData.fromString("3")));
        split1.add(insertEvent3);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        split1.add(addColumnEvent);

        // insert into table2
        insertEvent1 =
                DataChangeEvent.insertEvent(
                        table2,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1")));
        split1.add(insertEvent1);
        insertEvent2 =
                DataChangeEvent.insertEvent(
                        table2,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")));
        split1.add(insertEvent2);
        insertEvent3 =
                DataChangeEvent.insertEvent(
                        table2,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("3"),
                                GenericStringData.fromString("3")));
        split1.add(insertEvent3);

        // rename column
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        split1.add(renameColumnEvent);

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", DataTypes.STRING())));
        split1.add(dropColumnEvent);

        // delete
        split1.add(
                DataChangeEvent.deleteEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1"))));

        // update
        split1.add(
                DataChangeEvent.updateEvent(
                        table1,
                        RowType.of(DataTypes.STRING(), DataTypes.STRING()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("x"))));

        eventOfSplits.add(split1);
        return eventOfSplits;
    }
}
