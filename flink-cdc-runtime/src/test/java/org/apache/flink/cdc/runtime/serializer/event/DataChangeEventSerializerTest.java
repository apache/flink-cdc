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

package org.apache.flink.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link DataChangeEventSerializer}. */
class DataChangeEventSerializerTest extends SerializerTestBase<DataChangeEvent> {
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

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()));
        RecordData before =
                generator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("test"),
                            BinaryStringData.fromString("comment")
                        });
        RecordData after =
                generator.generate(
                        new Object[] {1L, null, BinaryStringData.fromString("updateComment")});
        return new DataChangeEvent[] {
            DataChangeEvent.insertEvent(TableId.tableId("table"), after),
            DataChangeEvent.insertEvent(TableId.tableId("table"), after, meta),
            DataChangeEvent.replaceEvent(TableId.tableId("schema", "table"), after),
            DataChangeEvent.replaceEvent(TableId.tableId("schema", "table"), after, meta),
            DataChangeEvent.deleteEvent(TableId.tableId("table"), before),
            DataChangeEvent.deleteEvent(TableId.tableId("table"), before, meta),
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), before, after),
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), before, after, meta)
        };
    }
}
