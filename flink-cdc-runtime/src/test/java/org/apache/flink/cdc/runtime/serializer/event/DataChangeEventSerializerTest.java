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
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.MapSerializer;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.data.RecordDataSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        RecordData before = generateBefore();
        RecordData after = generateAfter();
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
                    TableId.tableId("namespace", "schema", "table"), before, after, meta),
            // UPDATE with a null before image, as emitted in upsert changelog mode
            // (FLINK-38647).
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), null, after),
            DataChangeEvent.updateEvent(
                    TableId.tableId("namespace", "schema", "table"), null, after, meta)
        };
    }

    @Test
    void testDeserializeLegacyVersionDerivesPresenceFromOperationType() throws IOException {
        Map<String, String> meta = new HashMap<>();
        meta.put("option", "meta1");
        RecordData before = generateBefore();
        RecordData after = generateAfter();
        TableId tableId = TableId.tableId("schema", "table");

        // Pre-version-2 layout: op, tableId, records without presence flags, meta.
        EnumSerializer<OperationType> opSerializer = new EnumSerializer<>(OperationType.class);
        TypeSerializer<Map<String, String>> metaSerializer =
                new NullableSerializerWrapper<>(
                        new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE));
        DataOutputSerializer out = new DataOutputSerializer(256);
        opSerializer.serialize(OperationType.UPDATE, out);
        TableIdSerializer.INSTANCE.serialize(tableId, out);
        RecordDataSerializer.INSTANCE.serialize(before, out);
        RecordDataSerializer.INSTANCE.serialize(after, out);
        metaSerializer.serialize(meta, out);

        DataChangeEvent event =
                DataChangeEventSerializer.INSTANCE.deserialize(
                        1, new DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(event).isEqualTo(DataChangeEvent.updateEvent(tableId, before, after, meta));
    }

    @Test
    void testDeserializeUnknownVersion() {
        assertThatThrownBy(
                        () ->
                                DataChangeEventSerializer.INSTANCE.deserialize(
                                        3, new DataInputDeserializer(new byte[0])))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unrecognized serialization version");
    }

    @Test
    void testUpsertModeUpdateEventRoundTrip() throws IOException {
        RecordData after = generateAfter();
        DataChangeEvent event =
                DataChangeEvent.updateEvent(TableId.tableId("schema", "table"), null, after);

        DataOutputSerializer out = new DataOutputSerializer(256);
        DataChangeEventSerializer.INSTANCE.serialize(event, out);
        DataChangeEvent deserialized =
                DataChangeEventSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(deserialized).isEqualTo(event);
        assertThat(deserialized.before()).isNull();
    }

    private static RecordData generateBefore() {
        return generator()
                .generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("test"),
                            BinaryStringData.fromString("comment")
                        });
    }

    private static RecordData generateAfter() {
        return generator()
                .generate(new Object[] {1L, null, BinaryStringData.fromString("updateComment")});
    }

    private static BinaryRecordDataGenerator generator() {
        return new BinaryRecordDataGenerator(
                RowType.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()));
    }
}
