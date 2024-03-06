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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.MetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link MetadataColumn}. */
public class MetadataColumnSerializer extends TypeSerializerSingleton<MetadataColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final MetadataColumnSerializer INSTANCE = new MetadataColumnSerializer();

    private final DataTypeSerializer dataTypeSerializer = new DataTypeSerializer();
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public MetadataColumn createInstance() {
        return Column.metadataColumn("unknow", DataTypes.BIGINT());
    }

    @Override
    public MetadataColumn copy(MetadataColumn from) {
        return Column.metadataColumn(
                stringSerializer.copy(from.getName()),
                dataTypeSerializer.copy(from.getType()),
                stringSerializer.copy(from.getMetadataKey()),
                stringSerializer.copy(from.getComment()));
    }

    @Override
    public MetadataColumn copy(MetadataColumn from, MetadataColumn reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(MetadataColumn record, DataOutputView target) throws IOException {
        stringSerializer.serialize(record.getName(), target);
        dataTypeSerializer.serialize(record.getType(), target);
        stringSerializer.serialize(record.getComment(), target);
        stringSerializer.serialize(record.getMetadataKey(), target);
    }

    @Override
    public MetadataColumn deserialize(DataInputView source) throws IOException {
        String name = stringSerializer.deserialize(source);
        DataType dataType = dataTypeSerializer.deserialize(source);
        String comment = stringSerializer.deserialize(source);
        String metadataKey = stringSerializer.deserialize(source);
        return Column.metadataColumn(name, dataType, metadataKey, comment);
    }

    @Override
    public MetadataColumn deserialize(MetadataColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<MetadataColumn> snapshotConfiguration() {
        return new MetadataColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class MetadataColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<MetadataColumn> {

        public MetadataColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
