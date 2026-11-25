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

package org.apache.flink.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.ColumnPosition;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link ReorderColumnEvent.ColumnMove}. */
public class ColumnMoveSerializer extends TypeSerializerSingleton<ReorderColumnEvent.ColumnMove> {

    private static final long serialVersionUID = 1L;

    public static final ColumnMoveSerializer INSTANCE = new ColumnMoveSerializer();

    private final TypeSerializer<String> columnNameSerializer = StringSerializer.INSTANCE;
    private final EnumSerializer<ColumnPosition> positionSerializer =
            new EnumSerializer<>(ColumnPosition.class);
    private final TypeSerializer<String> referenceColumnNameSerializer =
            new NullableSerializerWrapper<>(StringSerializer.INSTANCE);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ReorderColumnEvent.ColumnMove createInstance() {
        return new ReorderColumnEvent.ColumnMove("unknown", ColumnPosition.FIRST, null);
    }

    @Override
    public ReorderColumnEvent.ColumnMove copy(ReorderColumnEvent.ColumnMove from) {
        return new ReorderColumnEvent.ColumnMove(
                columnNameSerializer.copy(from.getColumnName()),
                from.getPosition(),
                referenceColumnNameSerializer.copy(from.getReferenceColumnName()));
    }

    @Override
    public ReorderColumnEvent.ColumnMove copy(
            ReorderColumnEvent.ColumnMove from, ReorderColumnEvent.ColumnMove reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ReorderColumnEvent.ColumnMove record, DataOutputView target)
            throws IOException {
        columnNameSerializer.serialize(record.getColumnName(), target);
        positionSerializer.serialize(record.getPosition(), target);
        referenceColumnNameSerializer.serialize(record.getReferenceColumnName(), target);
    }

    @Override
    public ReorderColumnEvent.ColumnMove deserialize(DataInputView source) throws IOException {
        return new ReorderColumnEvent.ColumnMove(
                columnNameSerializer.deserialize(source),
                positionSerializer.deserialize(source),
                referenceColumnNameSerializer.deserialize(source));
    }

    @Override
    public ReorderColumnEvent.ColumnMove deserialize(
            ReorderColumnEvent.ColumnMove reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<ReorderColumnEvent.ColumnMove> snapshotConfiguration() {
        return new ColumnMoveSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ColumnMoveSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ReorderColumnEvent.ColumnMove> {

        public ColumnMoveSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
