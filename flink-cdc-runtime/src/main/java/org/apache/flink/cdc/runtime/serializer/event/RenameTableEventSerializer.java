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
import org.apache.flink.cdc.common.event.RenameTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.serializer.MapSerializer;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.DataTypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link RenameTableEvent}. */
public class RenameTableEventSerializer extends TypeSerializerSingleton<RenameTableEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final RenameTableEventSerializer INSTANCE = new RenameTableEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final MapSerializer<String, DataType> typeMapSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, new DataTypeSerializer());

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public RenameTableEvent createInstance() {
        return new RenameTableEvent(TableId.tableId("unknown"), TableId.tableId("unknown"));
    }

    @Override
    public RenameTableEvent copy(RenameTableEvent from) {
        return new RenameTableEvent(from.tableId(), from.newTableId());
    }

    @Override
    public RenameTableEvent copy(RenameTableEvent from, RenameTableEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RenameTableEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        tableIdSerializer.serialize(record.newTableId(), target);
    }

    @Override
    public RenameTableEvent deserialize(DataInputView source) throws IOException {
        return new RenameTableEvent(
                tableIdSerializer.deserialize(source), tableIdSerializer.deserialize(source));
    }

    @Override
    public RenameTableEvent deserialize(RenameTableEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<RenameTableEvent> snapshotConfiguration() {
        return new RenameTableEventSerializer.RenameTableEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class RenameTableEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RenameTableEvent> {

        public RenameTableEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
