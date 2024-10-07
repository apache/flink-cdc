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
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link RowType}. */
public class RowTypeSerializer extends TypeSerializerSingleton<RowType> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final RowTypeSerializer INSTANCE = new RowTypeSerializer();

    private volatile ListSerializer<DataField> fieldsSerializer;

    private ListSerializer<DataField> getFieldsSerializer() {
        if (fieldsSerializer == null) {
            synchronized (this) {
                if (fieldsSerializer == null) {
                    fieldsSerializer = new ListSerializer<>(DataFieldSerializer.INSTANCE);
                }
            }
        }
        return fieldsSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public RowType createInstance() {
        return new RowType(Collections.emptyList());
    }

    @Override
    public RowType copy(RowType from) {
        return new RowType(from.isNullable(), getFieldsSerializer().copy(from.getFields()));
    }

    @Override
    public RowType copy(RowType from, RowType reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RowType record, DataOutputView target) throws IOException {
        target.writeBoolean(record.isNullable());
        getFieldsSerializer().serialize(record.getFields(), target);
    }

    @Override
    public RowType deserialize(DataInputView source) throws IOException {
        boolean nullable = source.readBoolean();
        return new RowType(nullable, getFieldsSerializer().deserialize(source));
    }

    @Override
    public RowType deserialize(RowType reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<RowType> snapshotConfiguration() {
        return new RowTypeSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class RowTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RowType> {

        public RowTypeSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
