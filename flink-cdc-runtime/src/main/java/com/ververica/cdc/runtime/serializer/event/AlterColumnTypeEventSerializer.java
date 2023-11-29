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

import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.runtime.serializer.MapSerializer;
import com.ververica.cdc.runtime.serializer.StringSerializer;
import com.ververica.cdc.runtime.serializer.TableIdSerializer;
import com.ververica.cdc.runtime.serializer.TypeSerializerSingleton;
import com.ververica.cdc.runtime.serializer.schema.DataTypeSerializer;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link AlterColumnTypeEvent}. */
public class AlterColumnTypeEventSerializer extends TypeSerializerSingleton<AlterColumnTypeEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final AlterColumnTypeEventSerializer INSTANCE =
            new AlterColumnTypeEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final MapSerializer<String, DataType> typeMapSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, new DataTypeSerializer());

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AlterColumnTypeEvent createInstance() {
        return new AlterColumnTypeEvent(TableId.tableId("unknown"), Collections.emptyMap());
    }

    @Override
    public AlterColumnTypeEvent copy(AlterColumnTypeEvent from) {
        return new AlterColumnTypeEvent(
                from.tableId(), typeMapSerializer.copy(from.getTypeMapping()));
    }

    @Override
    public AlterColumnTypeEvent copy(AlterColumnTypeEvent from, AlterColumnTypeEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AlterColumnTypeEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        typeMapSerializer.serialize(record.getTypeMapping(), target);
    }

    @Override
    public AlterColumnTypeEvent deserialize(DataInputView source) throws IOException {
        return new AlterColumnTypeEvent(
                tableIdSerializer.deserialize(source), typeMapSerializer.deserialize(source));
    }

    @Override
    public AlterColumnTypeEvent deserialize(AlterColumnTypeEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<AlterColumnTypeEvent> snapshotConfiguration() {
        return new AlterColumnTypeEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class AlterColumnTypeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AlterColumnTypeEvent> {

        public AlterColumnTypeEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
