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

import com.ververica.cdc.common.data.binary.BinaryStringData;
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
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A helper class for {@link ValuesDataSource} to build events of each split.
 *
 * <p>the last list of getSourceEvents method is defined as the split for incremental stage.
 */
public class ValuesDataSourceHelper {

    /**
     * Different situations for creating sourceEvents, {@link ValuesDataSourceOptions#EVENT_SET_ID}.
     */
    public enum EventSetId {
        SINGLE_SPLIT_SINGLE_TABLE,
        SINGLE_SPLIT_MULTI_TABLES,
        MULTI_SPLITS_SINGLE_TABLE,
        CUSTOM_SOURCE_EVENTS
    }

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    public static final TableId TABLE_2 =
            TableId.tableId("default_namespace", "default_schema", "table2");

    /**
     * create events of {@link DataChangeEvent} and {@link SchemaChangeEvent} for {@link
     * ValuesDataSource}.
     */
    private static List<List<Event>> sourceEvents;

    public static List<List<Event>> getSourceEvents() {
        if (sourceEvents == null) {
            // use default enum of SINGLE_SPLIT_SINGLE_TABLE
            sourceEvents = singleSplitSingleTable();
        }
        return sourceEvents;
    }

    /** set sourceEvents using custom events. */
    public static void setSourceEvents(List<List<Event>> customSourceEvents) {
        sourceEvents = customSourceEvents;
    }

    /** set sourceEvents using predefined events. */
    public static void setSourceEvents(EventSetId eventType) {
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
            case MULTI_SPLITS_SINGLE_TABLE:
                {
                    sourceEvents = multiSplitsSingleTable();
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

    public static List<List<Event>> singleSplitSingleTable() {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split1.add(insertEvent3);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TABLE_1, Collections.singletonList(columnWithPosition));
        split1.add(addColumnEvent);

        // rename column
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(TABLE_1, nameMapping);
        split1.add(renameColumnEvent);

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        TABLE_1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", DataTypes.STRING())));
        split1.add(dropColumnEvent);

        // delete
        split1.add(
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                })));

        // update
        split1.add(
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("x")
                                })));

        eventOfSplits.add(split1);
        return eventOfSplits;
    }

    public static List<List<Event>> singleSplitMultiTables() {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        CreateTableEvent createTableEvent2 = new CreateTableEvent(TABLE_2, schema);
        split1.add(createTableEvent2);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        // insert into table1
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split1.add(insertEvent3);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TABLE_1, Collections.singletonList(columnWithPosition));
        split1.add(addColumnEvent);

        // insert into table2
        insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split1.add(insertEvent3);

        // rename column
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(TABLE_1, nameMapping);
        split1.add(renameColumnEvent);

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        TABLE_1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", DataTypes.STRING())));
        split1.add(dropColumnEvent);

        // delete
        split1.add(
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                })));

        // update
        split1.add(
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("x")
                                })));

        eventOfSplits.add(split1);
        return eventOfSplits;
    }

    public static List<List<Event>> multiSplitsSingleTable() {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));

        // create split1
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        eventOfSplits.add(split1);

        // create split2
        List<Event> split2 = new ArrayList<>();
        split2.add(createTableEvent);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split2.add(insertEvent3);
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("4")
                                }));
        split2.add(insertEvent4);
        eventOfSplits.add(split2);

        // create split3
        List<Event> split3 = new ArrayList<>();
        split3.add(createTableEvent);
        DataChangeEvent insertEvent5 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("5"),
                                    BinaryStringData.fromString("5")
                                }));
        split3.add(insertEvent5);
        DataChangeEvent insertEvent6 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("6"),
                                    BinaryStringData.fromString("6")
                                }));
        split3.add(insertEvent6);
        eventOfSplits.add(split3);

        // create split4
        List<Event> split4 = new ArrayList<>();
        split4.add(createTableEvent);
        DataChangeEvent deleteEvent1 =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split4.add(deleteEvent1);
        DataChangeEvent deleteEvent2 =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("4")
                                }));
        split4.add(deleteEvent2);
        DataChangeEvent deleteEvent3 =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("6"),
                                    BinaryStringData.fromString("6")
                                }));
        split4.add(deleteEvent3);
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TABLE_1, Collections.singletonList(columnWithPosition));
        split4.add(addColumnEvent);
        generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent updateEvent1 =
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("x")
                                }));
        split4.add(updateEvent1);
        DataChangeEvent updateEvent2 =
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("x")
                                }));
        split4.add(updateEvent2);
        eventOfSplits.add(split4);

        return eventOfSplits;
    }
}
