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
import org.apache.flink.cdc.common.data.RecordData;
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

    private static final int CURRENT_VERSION = 2;

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

        // Explicit presence flags: before may be null for UPDATE events in upsert changelog
        // mode (FLINK-38647), so record presence cannot be derived from the operation type.
        boolean beforePresent = event.before() != null;
        boolean afterPresent = event.after() != null;
        target.writeBoolean(beforePresent);
        target.writeBoolean(afterPresent);
        if (beforePresent) {
            recordDataSerializer.serialize(event.before(), target);
        }
        if (afterPresent) {
            recordDataSerializer.serialize(event.after(), target);
        }
        metaSerializer.serialize(event.meta(), target);
    }

    @Override
    public DataChangeEvent deserialize(DataInputView source) throws IOException {
        return deserialize(CURRENT_VERSION, source);
    }

    /**
     * De-serializes a {@link DataChangeEvent} written with the given serialization version.
     * Versions 0 and 1 derived record presence from the operation type; version 2 writes explicit
     * presence flags so UPDATE events with a null before image (upsert changelog mode, FLINK-38647)
     * round-trip correctly.
     */
    public DataChangeEvent deserialize(int version, DataInputView source) throws IOException {
        switch (version) {
            case 0:
            case 1:
                {
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
                            throw new IllegalArgumentException(
                                    "Unsupported data change event: " + op);
                    }
                }
            case 2:
                {
                    OperationType op = opSerializer.deserialize(source);
                    TableId tableId = tableIdSerializer.deserialize(source);

                    boolean beforePresent = source.readBoolean();
                    boolean afterPresent = source.readBoolean();
                    RecordData before =
                            beforePresent ? recordDataSerializer.deserialize(source) : null;
                    RecordData after =
                            afterPresent ? recordDataSerializer.deserialize(source) : null;

                    switch (op) {
                        case DELETE:
                            return DataChangeEvent.deleteEvent(
                                    tableId, before, metaSerializer.deserialize(source));
                        case INSERT:
                            return DataChangeEvent.insertEvent(
                                    tableId, after, metaSerializer.deserialize(source));
                        case UPDATE:
                            return DataChangeEvent.updateEvent(
                                    tableId, before, after, metaSerializer.deserialize(source));
                        case REPLACE:
                            return DataChangeEvent.replaceEvent(
                                    tableId, after, metaSerializer.deserialize(source));
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported data change event: " + op);
                    }
                }
            default:
                throw new IOException("Unrecognized serialization version " + version);
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
        // before may be null for UPDATE events in upsert changelog mode (FLINK-38647).
        RecordData before = from.before() != null ? recordDataSerializer.copy(from.before()) : null;
        RecordData after = from.after() != null ? recordDataSerializer.copy(from.after()) : null;
        TableId tableId = tableIdSerializer.copy(from.tableId());
        Map<String, String> meta = metaSerializer.copy(from.meta());
        switch (op) {
            case DELETE:
                return DataChangeEvent.deleteEvent(tableId, before, meta);
            case INSERT:
                return DataChangeEvent.insertEvent(tableId, after, meta);
            case UPDATE:
                return DataChangeEvent.updateEvent(tableId, before, after, meta);
            case REPLACE:
                return DataChangeEvent.replaceEvent(tableId, after, meta);
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
