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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.MapSerializer;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link Schema}. */
public class SchemaSerializer extends TypeSerializerSingleton<Schema> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final SchemaSerializer INSTANCE = new SchemaSerializer();

    private final ListSerializer<Column> columnsSerializer =
            new ListSerializer<>(ColumnSerializer.INSTANCE);
    private final ListSerializer<String> primaryKeysSerializer =
            new ListSerializer<>(StringSerializer.INSTANCE);
    private final ListSerializer<String> partitionKeysSerializer =
            new ListSerializer<>(StringSerializer.INSTANCE);
    private final MapSerializer<String, String> optionsSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE);
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Schema createInstance() {
        return Schema.newBuilder().build();
    }

    @Override
    public Schema copy(Schema from) {
        return Schema.newBuilder()
                .setColumns(columnsSerializer.copy(from.getColumns()))
                .primaryKey(primaryKeysSerializer.copy(from.primaryKeys()))
                .partitionKey(partitionKeysSerializer.copy(from.partitionKeys()))
                .options(optionsSerializer.copy(from.options()))
                .comment(stringSerializer.copy(from.comment()))
                .build();
    }

    @Override
    public Schema copy(Schema from, Schema reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Schema record, DataOutputView target) throws IOException {
        columnsSerializer.serialize(record.getColumns(), target);
        primaryKeysSerializer.serialize(record.primaryKeys(), target);
        partitionKeysSerializer.serialize(record.partitionKeys(), target);
        optionsSerializer.serialize(record.options(), target);
        stringSerializer.serialize(record.comment(), target);
    }

    private static final int CURRENT_VERSION = 2;

    @Override
    public Schema deserialize(DataInputView source) throws IOException {
        return deserialize(CURRENT_VERSION, source);
    }

    public Schema deserialize(int version, DataInputView source) throws IOException {
        // Manually updating versions because column deserialization is wrapped by
        // ListSerializer.
        ColumnSerializer.updateVersion(version);
        switch (version) {
            case 0:
                return Schema.newBuilder()
                        .setColumns(columnsSerializer.deserialize(source))
                        .primaryKey(primaryKeysSerializer.deserialize(source))
                        .options(optionsSerializer.deserialize(source))
                        .comment(stringSerializer.deserialize(source))
                        .build();
            case 1:
            case 2:
                return Schema.newBuilder()
                        .setColumns(columnsSerializer.deserialize(source))
                        .primaryKey(primaryKeysSerializer.deserialize(source))
                        .partitionKey(partitionKeysSerializer.deserialize(source))
                        .options(optionsSerializer.deserialize(source))
                        .comment(stringSerializer.deserialize(source))
                        .build();
            default:
                throw new IOException("Unrecognized serialization version " + version);
        }
    }

    @Override
    public Schema deserialize(Schema reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Schema> snapshotConfiguration() {
        return new SchemaSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SchemaSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Schema> {

        public SchemaSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
