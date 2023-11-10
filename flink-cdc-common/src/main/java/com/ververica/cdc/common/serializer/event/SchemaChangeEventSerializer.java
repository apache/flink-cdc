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

import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link SchemaChangeEvent}. */
public final class SchemaChangeEventSerializer extends TypeSerializer<SchemaChangeEvent> {
    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final SchemaChangeEventSerializer INSTANCE = new SchemaChangeEventSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<SchemaChangeEvent> duplicate() {
        return new SchemaChangeEventSerializer();
    }

    @Override
    public SchemaChangeEvent createInstance() {
        return () -> TableId.tableId("unknown", "unknown", "unknown");
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from) {
        return from;
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from, SchemaChangeEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(SchemaChangeEvent record, DataOutputView target) throws IOException {}

    @Override
    public SchemaChangeEvent deserialize(DataInputView source) throws IOException {
        // TODO
        return () -> TableId.tableId("unknown", "unknown", "unknown");
    }

    @Override
    public SchemaChangeEvent deserialize(SchemaChangeEvent reuse, DataInputView source)
            throws IOException {
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
    public TypeSerializerSnapshot<SchemaChangeEvent> snapshotConfiguration() {
        return new SchemaChangeEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SchemaChangeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<SchemaChangeEvent> {

        public SchemaChangeEventSerializerSnapshot() {
            super(SchemaChangeEventSerializer::new);
        }
    }

    enum SchemaChangeEventClass {
        ALTER_COLUMN_TYPE,
        RENAME_COLUMN,
        ADD_COLUMN,
        DROP_COLUMN,
        CREATE_TABLE;
    }
}
