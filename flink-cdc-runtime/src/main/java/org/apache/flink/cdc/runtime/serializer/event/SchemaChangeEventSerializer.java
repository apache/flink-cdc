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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;

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
            public SchemaChangeEventType getType() {
                return null;
            }

            @Override
            public TableId tableId() {
                return TableId.tableId("unknown", "unknown", "unknown");
            }

            @Override
            public SchemaChangeEvent copy(TableId newTableId) {
                return null;
            }
        };
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from) {
        return SchemaChangeEventVisitor.visit(
                from,
                AddColumnEventSerializer.INSTANCE::copy,
                AlterColumnTypeEventSerializer.INSTANCE::copy,
                CreateTableEventSerializer.INSTANCE::copy,
                DropColumnEventSerializer.INSTANCE::copy,
                DropTableEventSerializer.INSTANCE::copy,
                RenameColumnEventSerializer.INSTANCE::copy,
                TruncateTableEventSerializer.INSTANCE::copy);
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

        SchemaChangeEventVisitor.<Void, IOException>visit(
                record,
                addColumnEvent -> {
                    enumSerializer.serialize(ADD_COLUMN, target);
                    AddColumnEventSerializer.INSTANCE.serialize(addColumnEvent, target);
                    return null;
                },
                alterColumnTypeEvent -> {
                    enumSerializer.serialize(ALTER_COLUMN_TYPE, target);
                    AlterColumnTypeEventSerializer.INSTANCE.serialize(alterColumnTypeEvent, target);
                    return null;
                },
                createTableEvent -> {
                    enumSerializer.serialize(CREATE_TABLE, target);
                    CreateTableEventSerializer.INSTANCE.serialize(createTableEvent, target);
                    return null;
                },
                dropColumnEvent -> {
                    enumSerializer.serialize(DROP_COLUMN, target);
                    DropColumnEventSerializer.INSTANCE.serialize(dropColumnEvent, target);
                    return null;
                },
                dropTableEvent -> {
                    enumSerializer.serialize(DROP_TABLE, target);
                    DropTableEventSerializer.INSTANCE.serialize(dropTableEvent, target);
                    return null;
                },
                renameColumnEvent -> {
                    enumSerializer.serialize(RENAME_COLUMN, target);
                    RenameColumnEventSerializer.INSTANCE.serialize(renameColumnEvent, target);
                    return null;
                },
                truncateTableEvent -> {
                    enumSerializer.serialize(TRUNCATE_TABLE, target);
                    TruncateTableEventSerializer.INSTANCE.serialize(truncateTableEvent, target);
                    return null;
                });
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
            case DROP_TABLE:
                return DropTableEventSerializer.INSTANCE.deserialize(source);
            case TRUNCATE_TABLE:
                return TruncateTableEventSerializer.INSTANCE.deserialize(source);
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
