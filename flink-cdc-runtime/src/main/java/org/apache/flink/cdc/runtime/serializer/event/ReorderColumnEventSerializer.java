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
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.ColumnMoveSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link ReorderColumnEvent}. */
public class ReorderColumnEventSerializer extends TypeSerializerSingleton<ReorderColumnEvent> {

    private static final long serialVersionUID = 1L;

    public static final ReorderColumnEventSerializer INSTANCE = new ReorderColumnEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final ListSerializer<ReorderColumnEvent.ColumnMove> columnMovesSerializer =
            new ListSerializer<>(ColumnMoveSerializer.INSTANCE);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ReorderColumnEvent createInstance() {
        return new ReorderColumnEvent(TableId.tableId("unknown"), Collections.emptyList());
    }

    @Override
    public ReorderColumnEvent copy(ReorderColumnEvent from) {
        return new ReorderColumnEvent(
                from.tableId(), columnMovesSerializer.copy(from.getColumnMoves()));
    }

    @Override
    public ReorderColumnEvent copy(ReorderColumnEvent from, ReorderColumnEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ReorderColumnEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        columnMovesSerializer.serialize(record.getColumnMoves(), target);
    }

    @Override
    public ReorderColumnEvent deserialize(DataInputView source) throws IOException {
        return new ReorderColumnEvent(
                tableIdSerializer.deserialize(source), columnMovesSerializer.deserialize(source));
    }

    @Override
    public ReorderColumnEvent deserialize(ReorderColumnEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<ReorderColumnEvent> snapshotConfiguration() {
        return new ReorderColumnEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ReorderColumnEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ReorderColumnEvent> {

        public ReorderColumnEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
