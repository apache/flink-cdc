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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link AddColumnEvent.ColumnWithPosition}. */
public class ColumnWithPositionSerializer
        extends TypeSerializerSingleton<AddColumnEvent.ColumnWithPosition> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final ColumnWithPositionSerializer INSTANCE = new ColumnWithPositionSerializer();

    private final TypeSerializer<Column> addColumnSerializer =
            new NullableSerializerWrapper<>(ColumnSerializer.INSTANCE);

    private final TypeSerializer<String> existedColumnNameSerializer = StringSerializer.INSTANCE;

    private final EnumSerializer<AddColumnEvent.ColumnPosition> positionEnumSerializer =
            new EnumSerializer<>(AddColumnEvent.ColumnPosition.class);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AddColumnEvent.ColumnWithPosition createInstance() {
        return new AddColumnEvent.ColumnWithPosition(
                Column.physicalColumn("unknown", DataTypes.BIGINT()));
    }

    @Override
    public AddColumnEvent.ColumnWithPosition copy(AddColumnEvent.ColumnWithPosition from) {
        return new AddColumnEvent.ColumnWithPosition(
                addColumnSerializer.copy(from.getAddColumn()),
                from.getPosition(),
                existedColumnNameSerializer.copy(from.getExistedColumnName()));
    }

    @Override
    public AddColumnEvent.ColumnWithPosition copy(
            AddColumnEvent.ColumnWithPosition from, AddColumnEvent.ColumnWithPosition reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AddColumnEvent.ColumnWithPosition record, DataOutputView target)
            throws IOException {
        addColumnSerializer.serialize(record.getAddColumn(), target);
        positionEnumSerializer.serialize(record.getPosition(), target);
        existedColumnNameSerializer.serialize(record.getExistedColumnName(), target);
    }

    @Override
    public AddColumnEvent.ColumnWithPosition deserialize(DataInputView source) throws IOException {
        Column addColumn = addColumnSerializer.deserialize(source);
        AddColumnEvent.ColumnPosition position = positionEnumSerializer.deserialize(source);
        return new AddColumnEvent.ColumnWithPosition(
                addColumn, position, existedColumnNameSerializer.deserialize(source));
    }

    @Override
    public AddColumnEvent.ColumnWithPosition deserialize(
            AddColumnEvent.ColumnWithPosition reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<AddColumnEvent.ColumnWithPosition> snapshotConfiguration() {
        return new ColumnWithPositionSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ColumnWithPositionSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AddColumnEvent.ColumnWithPosition> {

        public ColumnWithPositionSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
