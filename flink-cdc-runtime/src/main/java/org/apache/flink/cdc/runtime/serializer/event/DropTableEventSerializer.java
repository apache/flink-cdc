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
import org.apache.flink.cdc.common.event.DropTableEvent;
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

/** A {@link TypeSerializer} for {@link DropTableEvent}. */
public class DropTableEventSerializer extends TypeSerializerSingleton<DropTableEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DropTableEventSerializer INSTANCE = new DropTableEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final MapSerializer<String, DataType> typeMapSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, new DataTypeSerializer());

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public DropTableEvent createInstance() {
        return new DropTableEvent(TableId.tableId("unknown"));
    }

    @Override
    public DropTableEvent copy(DropTableEvent from) {
        return new DropTableEvent(from.tableId());
    }

    @Override
    public DropTableEvent copy(DropTableEvent from, DropTableEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DropTableEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
    }

    @Override
    public DropTableEvent deserialize(DataInputView source) throws IOException {
        return new DropTableEvent(tableIdSerializer.deserialize(source));
    }

    @Override
    public DropTableEvent deserialize(DropTableEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<DropTableEvent> snapshotConfiguration() {
        return new DropTableEventSerializer.DropTableEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DropTableEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DropTableEvent> {

        public DropTableEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
