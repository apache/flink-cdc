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

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link DataChangeEventSerializer}. */
public class DataChangeEventSerializerTest extends SerializerTestBase<DataChangeEvent> {
    @Override
    protected TypeSerializer<DataChangeEvent> createSerializer() {
        return DataChangeEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<DataChangeEvent> getTypeClass() {
        return DataChangeEvent.class;
    }

    @Override
    protected DataChangeEvent[] getTestData() {
        Map<String, String> meta = new HashMap<>();
        meta.put("option", "meta1");
        RecordData before =
                GenericRecordData.of(
                        1L,
                        GenericStringData.fromString("test"),
                        GenericStringData.fromString("comment"));
        RecordData after =
                GenericRecordData.of(1L, null, GenericStringData.fromString("updateComment"));
        RowType rowType = RowType.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING());
        return new DataChangeEvent[] {
            DataChangeEvent.insertEvent(TableId.tableId("table"), rowType, after),
            DataChangeEvent.insertEvent(TableId.tableId("table"), rowType, after, meta),
            DataChangeEvent.replaceEvent(TableId.tableId("schema", "table"), rowType, after),
            DataChangeEvent.replaceEvent(TableId.tableId("schema", "table"), rowType, after, meta),
            DataChangeEvent.deleteEvent(TableId.tableId("table"), rowType, before),
            DataChangeEvent.deleteEvent(TableId.tableId("table"), rowType, before, meta),
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), rowType, before, after),
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), rowType, before, after, meta)
        };
    }
}
