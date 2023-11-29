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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.runtime.serializer.EnumSerializer;
import com.ververica.cdc.runtime.serializer.TableIdSerializer;
import com.ververica.cdc.runtime.serializer.TypeSerializerSingleton;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link Event}. */
public final class EventSerializer extends TypeSerializerSingleton<Event> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final EventSerializer INSTANCE = new EventSerializer();

    private final SchemaChangeEventSerializer schemaChangeEventSerializer =
            SchemaChangeEventSerializer.INSTANCE;
    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final EnumSerializer<EventClass> enumSerializer =
            new EnumSerializer<>(EventClass.class);
    private final TypeSerializer<DataChangeEvent> dataChangeEventSerializer =
            DataChangeEventSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Event createInstance() {
        return new Event() {};
    }

    @Override
    public Event copy(Event from) {
        if (from instanceof FlushEvent) {
            return new FlushEvent(tableIdSerializer.copy(((FlushEvent) from).getTableId()));
        } else if (from instanceof SchemaChangeEvent) {
            return schemaChangeEventSerializer.copy((SchemaChangeEvent) from);
        } else if (from instanceof DataChangeEvent) {
            return dataChangeEventSerializer.copy((DataChangeEvent) from);
        }
        throw new UnsupportedOperationException("Unknown event type: " + from.toString());
    }

    @Override
    public Event copy(Event from, Event reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Event record, DataOutputView target) throws IOException {
        if (record instanceof FlushEvent) {
            enumSerializer.serialize(EventClass.FLUSH_EVENT, target);
            tableIdSerializer.serialize(((FlushEvent) record).getTableId(), target);
        } else if (record instanceof SchemaChangeEvent) {
            enumSerializer.serialize(EventClass.SCHEME_CHANGE_EVENT, target);
            schemaChangeEventSerializer.serialize((SchemaChangeEvent) record, target);
        } else if (record instanceof DataChangeEvent) {
            enumSerializer.serialize(EventClass.DATA_CHANGE_EVENT, target);
            dataChangeEventSerializer.serialize((DataChangeEvent) record, target);
        } else {
            throw new UnsupportedOperationException("Unknown event type: " + record.toString());
        }
    }

    @Override
    public Event deserialize(DataInputView source) throws IOException {
        EventClass eventClass = enumSerializer.deserialize(source);
        switch (eventClass) {
            case FLUSH_EVENT:
                return new FlushEvent(tableIdSerializer.deserialize(source));
            case DATA_CHANGE_EVENT:
                return dataChangeEventSerializer.deserialize(source);
            case SCHEME_CHANGE_EVENT:
                return schemaChangeEventSerializer.deserialize(source);
            default:
                throw new UnsupportedOperationException("Unknown event type: " + eventClass);
        }
    }

    @Override
    public Event deserialize(Event reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Event> snapshotConfiguration() {
        return new EventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class EventSerializerSnapshot extends SimpleTypeSerializerSnapshot<Event> {

        public EventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }

    enum EventClass {
        DATA_CHANGE_EVENT,
        SCHEME_CHANGE_EVENT,
        FLUSH_EVENT
    }
}
