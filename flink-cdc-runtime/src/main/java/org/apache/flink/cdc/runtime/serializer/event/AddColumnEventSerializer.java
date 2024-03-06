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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.ColumnWithPositionSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link AddColumnEvent}. */
public class AddColumnEventSerializer extends TypeSerializerSingleton<AddColumnEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final AddColumnEventSerializer INSTANCE = new AddColumnEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final ListSerializer<AddColumnEvent.ColumnWithPosition> columnsSerializer =
            new ListSerializer<>(ColumnWithPositionSerializer.INSTANCE);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AddColumnEvent createInstance() {
        return new AddColumnEvent(TableId.tableId("unknown"), Collections.emptyList());
    }

    @Override
    public AddColumnEvent copy(AddColumnEvent from) {
        return new AddColumnEvent(from.tableId(), columnsSerializer.copy(from.getAddedColumns()));
    }

    @Override
    public AddColumnEvent copy(AddColumnEvent from, AddColumnEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AddColumnEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        columnsSerializer.serialize(record.getAddedColumns(), target);
    }

    @Override
    public AddColumnEvent deserialize(DataInputView source) throws IOException {
        return new AddColumnEvent(
                tableIdSerializer.deserialize(source), columnsSerializer.deserialize(source));
    }

    @Override
    public AddColumnEvent deserialize(AddColumnEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<AddColumnEvent> snapshotConfiguration() {
        return new AddColumnEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class AddColumnEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AddColumnEvent> {

        public AddColumnEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
