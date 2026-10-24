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
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for the {@link DataChangeEventSerializer}. */
class DataChangeEventSerializerTest extends SerializerTestBase<DataChangeEvent> {

    @Test
    void testLegacyOperationGoldenBytesRemainStable() throws Exception {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));
        RecordData before =
                generator.generate(new Object[] {1L, BinaryStringData.fromString("before")});
        RecordData after =
                generator.generate(new Object[] {2L, BinaryStringData.fromString("after")});
        TableId tableId = TableId.tableId("ns", "schema", "table");
        Map<String, String> meta = new HashMap<>();
        meta.put("source", "golden");

        List<String> actual =
                Arrays.asList(
                        serializeHex(DataChangeEvent.insertEvent(tableId, after, meta)),
                        serializeHex(DataChangeEvent.updateEvent(tableId, before, after, meta)),
                        serializeHex(DataChangeEvent.replaceEvent(tableId, after, meta)),
                        serializeHex(DataChangeEvent.deleteEvent(tableId, before, meta)));

        assertThat(actual)
                .containsExactly(
                        "000000000000000300026e730006736368656d6100057461626c65000000000200000018000000000000000002000000000000006166746572000085000000000107736f757263650007676f6c64656e",
                        "000000010000000300026e730006736368656d6100057461626c65000000000200000018000000000000000001000000000000006265666f72650086000000000200000018000000000000000002000000000000006166746572000085000000000107736f757263650007676f6c64656e",
                        "000000020000000300026e730006736368656d6100057461626c65000000000200000018000000000000000002000000000000006166746572000085000000000107736f757263650007676f6c64656e",
                        "000000030000000300026e730006736368656d6100057461626c65000000000200000018000000000000000001000000000000006265666f72650086000000000107736f757263650007676f6c64656e");
    }

    @Test
    void testUpdateBeforeRoundTripCopyAndReuse() throws Exception {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));
        RecordData before =
                generator.generate(new Object[] {1L, BinaryStringData.fromString("before-update")});
        Map<String, String> meta = new HashMap<>();
        meta.put("source", "update-before");
        DataChangeEvent event =
                DataChangeEvent.updateBeforeEvent(TableId.tableId("schema", "table"), before, meta);

        DataOutputSerializer output = new DataOutputSerializer(128);
        DataChangeEventSerializer.INSTANCE.serialize(event, output);
        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
        DataChangeEvent deserialized = DataChangeEventSerializer.INSTANCE.deserialize(input);

        assertThat(deserialized).isEqualTo(event);
        assertThat(deserialized.op()).isEqualTo(OperationType.UPDATE_BEFORE);
        assertThat(deserialized.before()).isEqualTo(before);
        assertThat(deserialized.after()).isNull();
        assertThat(deserialized.meta()).containsEntry("source", "update-before");
        assertThat(deserialized.opTypeString(false)).isEqualTo("-U");

        DataChangeEvent copied = DataChangeEventSerializer.INSTANCE.copy(event);
        assertThat(copied).isEqualTo(event).isNotSameAs(event);
        assertThat(copied.before()).isNotSameAs(event.before());

        DataChangeEvent reuse = DataChangeEvent.insertEvent(TableId.tableId("reuse"), before, meta);
        DataChangeEvent reused =
                DataChangeEventSerializer.INSTANCE.deserialize(
                        reuse, new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(reused).isEqualTo(event).isNotSameAs(reuse);
    }

    @Test
    void testUpdateBeforeRejectsTruncatedInput() throws Exception {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.BIGINT()));
        DataChangeEvent event =
                DataChangeEvent.updateBeforeEvent(
                        TableId.tableId("table"), generator.generate(new Object[] {1L}));
        DataOutputSerializer output = new DataOutputSerializer(64);
        DataChangeEventSerializer.INSTANCE.serialize(event, output);
        byte[] truncated = Arrays.copyOf(output.getCopyOfBuffer(), output.length() - 1);

        assertThatThrownBy(
                        () ->
                                DataChangeEventSerializer.INSTANCE.deserialize(
                                        new DataInputDeserializer(truncated)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testOperationOrdinalsRemainCompatible() {
        assertThat(OperationType.INSERT.ordinal()).isZero();
        assertThat(OperationType.UPDATE.ordinal()).isEqualTo(1);
        assertThat(OperationType.REPLACE.ordinal()).isEqualTo(2);
        assertThat(OperationType.DELETE.ordinal()).isEqualTo(3);
        assertThat(OperationType.UPDATE_BEFORE.ordinal()).isEqualTo(4);
    }

    private static String serializeHex(DataChangeEvent event) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(128);
        DataChangeEventSerializer.INSTANCE.serialize(event, output);
        StringBuilder hex = new StringBuilder(output.length() * 2);
        for (byte value : output.getCopyOfBuffer()) {
            hex.append(String.format("%02x", value & 0xff));
        }
        return hex.toString();
    }

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
                    TableId.tableId("namespace", "schema", "table"), before, after, meta),
            DataChangeEvent.updateBeforeEvent(TableId.tableId("table"), before),
            DataChangeEvent.updateBeforeEvent(TableId.tableId("table"), before, meta)
        };
    }
}
