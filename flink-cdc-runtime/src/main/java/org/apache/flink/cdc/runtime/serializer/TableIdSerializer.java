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

package org.apache.flink.cdc.runtime.serializer;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link TableId}. */
public final class TableIdSerializer extends TypeSerializer<TableId> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final TableIdSerializer INSTANCE = new TableIdSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<TableId> duplicate() {
        return new TableIdSerializer();
    }

    @Override
    public TableId createInstance() {
        return TableId.tableId("unknown", "unknown", "unknown");
    }

    @Override
    public TableId copy(TableId from) {
        return from;
    }

    @Override
    public TableId copy(TableId from, TableId reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(TableId record, DataOutputView target) throws IOException {
        int parts = 1;
        parts += record.getNamespace() == null ? 0 : 1;
        parts += record.getSchemaName() == null ? 0 : 1;
        target.writeInt(parts);
        if (record.getNamespace() != null) {
            target.writeUTF(record.getNamespace());
        }
        if (record.getSchemaName() != null) {
            target.writeUTF(record.getSchemaName());
        }
        target.writeUTF(record.getTableName());
    }

    @Override
    public TableId deserialize(DataInputView source) throws IOException {
        int parts = source.readInt();
        if (parts == 3) {
            return TableId.tableId(source.readUTF(), source.readUTF(), source.readUTF());
        }
        if (parts == 2) {
            return TableId.tableId(source.readUTF(), source.readUTF());
        }
        return TableId.tableId(source.readUTF());
    }

    @Override
    public TableId deserialize(TableId reuse, DataInputView source) throws IOException {
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
    public TypeSerializerSnapshot<TableId> snapshotConfiguration() {
        return new TableIdSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class TableIdSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<TableId> {

        public TableIdSerializerSnapshot() {
            super(TableIdSerializer::new);
        }
    }
}
