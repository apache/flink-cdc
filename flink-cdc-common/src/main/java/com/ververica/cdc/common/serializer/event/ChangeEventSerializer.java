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

package com.ververica.cdc.common.serializer.event;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.TableId;

import java.io.IOException;

/** TODO : A {@link TypeSerializer} for {@link ChangeEvent}. */
public final class ChangeEventSerializer extends TypeSerializer<ChangeEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final ChangeEventSerializer INSTANCE = new ChangeEventSerializer();

    private final SchemaChangeEventSerializer schemaChangeEventSerializer =
            SchemaChangeEventSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ChangeEvent> duplicate() {
        return new ChangeEventSerializer();
    }

    @Override
    public ChangeEvent createInstance() {
        return () -> TableId.tableId("unknown", "unknown", "unknown");
    }

    @Override
    public ChangeEvent copy(ChangeEvent from) {
        return from;
    }

    @Override
    public ChangeEvent copy(ChangeEvent from, ChangeEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ChangeEvent record, DataOutputView target) throws IOException {
        // TODO
    }

    @Override
    public ChangeEvent deserialize(DataInputView source) throws IOException {
        // TODO
        return () -> TableId.tableId("unknown", "unknown", "unknown");
    }

    @Override
    public ChangeEvent deserialize(ChangeEvent reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj != null && obj.getClass() == getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public TypeSerializerSnapshot<ChangeEvent> snapshotConfiguration() {
        return new ChangeEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ChangeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ChangeEvent> {

        public ChangeEventSerializerSnapshot() {
            super(ChangeEventSerializer::new);
        }
    }

    enum ChangeEventClass {
        DATA_CHANGE_EVENT,
        SCHEME_CHANGE_EVENT
    }
}
