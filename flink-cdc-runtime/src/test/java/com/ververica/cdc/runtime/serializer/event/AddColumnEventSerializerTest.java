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

package com.ververica.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;

/** A test for the {@link AddColumnEventSerializer}. */
public class AddColumnEventSerializerTest extends SerializerTestBase<AddColumnEvent> {
    @Override
    protected TypeSerializer<AddColumnEvent> createSerializer() {
        return AddColumnEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<AddColumnEvent> getTypeClass() {
        return AddColumnEvent.class;
    }

    @Override
    protected AddColumnEvent[] getTestData() {
        return new AddColumnEvent[] {
            new AddColumnEvent(
                    TableId.tableId("table"),
                    Arrays.asList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP())),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("testCol2", DataTypes.DOUBLE(), "desc"),
                                    AddColumnEvent.ColumnPosition.AFTER,
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP())))),
            new AddColumnEvent(
                    TableId.tableId("schema", "table"),
                    Arrays.asList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.metadataColumn("testCol1", DataTypes.TIMESTAMP()),
                                    AddColumnEvent.ColumnPosition.FIRST,
                                    null),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.metadataColumn("testCol2", DataTypes.DOUBLE(), "mKey"),
                                    AddColumnEvent.ColumnPosition.BEFORE,
                                    Column.metadataColumn("testCol1", DataTypes.TIMESTAMP())))),
            new AddColumnEvent(
                    TableId.tableId("namespace", "schema", "table"),
                    Arrays.asList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP()),
                                    AddColumnEvent.ColumnPosition.FIRST,
                                    null),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.metadataColumn(
                                            "testCol2", DataTypes.DOUBLE(), "mKey", "desc"),
                                    AddColumnEvent.ColumnPosition.BEFORE,
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP()))))
        };
    }
}
