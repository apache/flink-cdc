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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link SchemaChangeEvent}. */
public final class SchemaChangeEventSerializer extends TypeSerializerSingleton<SchemaChangeEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final SchemaChangeEventSerializer INSTANCE = new SchemaChangeEventSerializer();

    private final EnumSerializer<SchemaChangeEventType> enumSerializer =
            new EnumSerializer<>(SchemaChangeEventType.class);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public SchemaChangeEvent createInstance() {
        return new SchemaChangeEvent() {
            @Override
            public TableId tableId() {
                return TableId.tableId("unknown", "unknown", "unknown");
            }

            @Override
            public SchemaChangeEventType getType() {
                return null;
            }
        };
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from) {
        if (from instanceof AlterColumnTypeEvent) {
            return AlterColumnTypeEventSerializer.INSTANCE.copy((AlterColumnTypeEvent) from);
        } else if (from instanceof CreateTableEvent) {
            return CreateTableEventSerializer.INSTANCE.copy((CreateTableEvent) from);
        } else if (from instanceof RenameColumnEvent) {
            return RenameColumnEventSerializer.INSTANCE.copy((RenameColumnEvent) from);
        } else if (from instanceof AddColumnEvent) {
            return AddColumnEventSerializer.INSTANCE.copy((AddColumnEvent) from);
        } else if (from instanceof DropColumnEvent) {
            return DropColumnEventSerializer.INSTANCE.copy((DropColumnEvent) from);
        } else {
            throw new IllegalArgumentException("Unknown schema change event: " + from);
        }
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
    public void serialize(SchemaChangeEvent record, DataOutputView target) throws IOException {
        if (record instanceof AlterColumnTypeEvent) {
            enumSerializer.serialize(SchemaChangeEventType.ALTER_COLUMN_TYPE, target);
            AlterColumnTypeEventSerializer.INSTANCE.serialize(
                    (AlterColumnTypeEvent) record, target);
        } else if (record instanceof CreateTableEvent) {
            enumSerializer.serialize(SchemaChangeEventType.CREATE_TABLE, target);
            CreateTableEventSerializer.INSTANCE.serialize((CreateTableEvent) record, target);
        } else if (record instanceof RenameColumnEvent) {
            enumSerializer.serialize(SchemaChangeEventType.RENAME_COLUMN, target);
            RenameColumnEventSerializer.INSTANCE.serialize((RenameColumnEvent) record, target);
        } else if (record instanceof AddColumnEvent) {
            enumSerializer.serialize(SchemaChangeEventType.ADD_COLUMN, target);
            AddColumnEventSerializer.INSTANCE.serialize((AddColumnEvent) record, target);
        } else if (record instanceof DropColumnEvent) {
            enumSerializer.serialize(SchemaChangeEventType.DROP_COLUMN, target);
            DropColumnEventSerializer.INSTANCE.serialize((DropColumnEvent) record, target);
        } else {
            throw new IllegalArgumentException("Unknown schema change event: " + record);
        }
    }

    @Override
    public SchemaChangeEvent deserialize(DataInputView source) throws IOException {
        SchemaChangeEventType schemaChangeEventType = enumSerializer.deserialize(source);
        switch (schemaChangeEventType) {
            case ADD_COLUMN:
                return AddColumnEventSerializer.INSTANCE.deserialize(source);
            case DROP_COLUMN:
                return DropColumnEventSerializer.INSTANCE.deserialize(source);
            case CREATE_TABLE:
                return CreateTableEventSerializer.INSTANCE.deserialize(source);
            case RENAME_COLUMN:
                return RenameColumnEventSerializer.INSTANCE.deserialize(source);
            case ALTER_COLUMN_TYPE:
                return AlterColumnTypeEventSerializer.INSTANCE.deserialize(source);
            default:
                throw new IllegalArgumentException(
                        "Unknown schema change event class: " + schemaChangeEventType);
        }
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
    public TypeSerializerSnapshot<SchemaChangeEvent> snapshotConfiguration() {
        return new SchemaChangeEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SchemaChangeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<SchemaChangeEvent> {

        public SchemaChangeEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
