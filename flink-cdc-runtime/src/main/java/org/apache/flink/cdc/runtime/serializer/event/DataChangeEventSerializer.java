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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.MapSerializer;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.data.RecordDataSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

/** A {@link TypeSerializer} for {@link DataChangeEvent}. */
public class DataChangeEventSerializer extends TypeSerializerSingleton<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DataChangeEventSerializer INSTANCE = new DataChangeEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final TypeSerializer<Map<String, String>> metaSerializer =
            new NullableSerializerWrapper<>(
                    new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE));
    private final EnumSerializer<OperationType> opSerializer =
            new EnumSerializer<>(OperationType.class);
    private final RecordDataSerializer recordDataSerializer = RecordDataSerializer.INSTANCE;

    @Override
    public DataChangeEvent createInstance() {
        return DataChangeEvent.deleteEvent(TableId.tableId("unknown"), null);
    }

    @Override
    public void serialize(DataChangeEvent event, DataOutputView target) throws IOException {
        opSerializer.serialize(event.op(), target);
        tableIdSerializer.serialize(event.tableId(), target);

        if (event.before() != null) {
            recordDataSerializer.serialize(event.before(), target);
        }
        if (event.after() != null) {
            recordDataSerializer.serialize(event.after(), target);
        }
        metaSerializer.serialize(event.meta(), target);
    }

    @Override
    public DataChangeEvent deserialize(DataInputView source) throws IOException {
        OperationType op = opSerializer.deserialize(source);
        TableId tableId = tableIdSerializer.deserialize(source);

        switch (op) {
            case DELETE:
                return DataChangeEvent.deleteEvent(
                        tableId,
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case INSERT:
                return DataChangeEvent.insertEvent(
                        tableId,
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case UPDATE:
                return DataChangeEvent.updateEvent(
                        tableId,
                        recordDataSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case REPLACE:
                return DataChangeEvent.replaceEvent(
                        tableId,
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            default:
                throw new IllegalArgumentException("Unsupported data change event: " + op);
        }
    }

    @Override
    public DataChangeEvent deserialize(DataChangeEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public DataChangeEvent copy(DataChangeEvent from) {
        OperationType op = from.op();
        switch (op) {
            case DELETE:
                return DataChangeEvent.deleteEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.before()),
                        metaSerializer.copy(from.meta()));
            case INSERT:
                return DataChangeEvent.insertEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            case UPDATE:
                return DataChangeEvent.updateEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.before()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            case REPLACE:
                return DataChangeEvent.replaceEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            default:
                throw new IllegalArgumentException("Unsupported data change event: " + op);
        }
    }

    @Override
    public DataChangeEvent copy(DataChangeEvent from, DataChangeEvent reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<DataChangeEvent> snapshotConfiguration() {
        return new DataChangeEventSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link DataChangeEventSerializer}. */
    public static final class DataChangeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DataChangeEvent> {

        public DataChangeEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
