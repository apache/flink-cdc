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

package com.ververica.cdc.common.serializer.schema;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.serializer.ListSerializer;
import com.ververica.cdc.common.serializer.MapSerializer;
import com.ververica.cdc.common.serializer.StringSerializer;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link Schema}. */
public class SchemaSerializer extends TypeSerializer<Schema> {
    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final SchemaSerializer INSTANCE =
            new SchemaSerializer(
                    new ListSerializer<>(ColumnSerializer.INSTANCE),
                    new ListSerializer<>(StringSerializer.INSTANCE),
                    new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE),
                    StringSerializer.INSTANCE);

    private final ListSerializer<Column> columnsSerializer;
    private final ListSerializer<String> primaryKeysSerializer;
    private final MapSerializer<String, String> optionsSerializer;
    private final StringSerializer stringSerializer;

    public SchemaSerializer(
            ListSerializer<Column> columnsSerializer,
            ListSerializer<String> primaryKeysSerializer,
            MapSerializer<String, String> optionsSerializer,
            StringSerializer stringSerializer) {
        this.columnsSerializer = columnsSerializer;
        this.primaryKeysSerializer = primaryKeysSerializer;
        this.optionsSerializer = optionsSerializer;
        this.stringSerializer = stringSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Schema> duplicate() {
        return new SchemaSerializer(
                new ListSerializer<>(ColumnSerializer.INSTANCE),
                new ListSerializer<>(StringSerializer.INSTANCE),
                new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE),
                StringSerializer.INSTANCE);
    }

    @Override
    public Schema createInstance() {
        return Schema.newBuilder().build();
    }

    @Override
    public Schema copy(Schema from) {
        return from;
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
        optionsSerializer.serialize(record.options(), target);
        stringSerializer.serialize(record.comment(), target);
    }

    @Override
    public Schema deserialize(DataInputView source) throws IOException {
        return Schema.newBuilder()
                .setColumns(columnsSerializer.deserialize(source))
                .primaryKey(primaryKeysSerializer.deserialize(source))
                .options(optionsSerializer.deserialize(source))
                .comment(stringSerializer.deserialize(source))
                .build();
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
    public boolean equals(Object obj) {
        return obj == this || (obj != null && obj.getClass() == getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
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
            super(
                    () ->
                            new SchemaSerializer(
                                    new ListSerializer<>(ColumnSerializer.INSTANCE),
                                    new ListSerializer<>(StringSerializer.INSTANCE),
                                    new MapSerializer<>(
                                            StringSerializer.INSTANCE, StringSerializer.INSTANCE),
                                    StringSerializer.INSTANCE));
        }
    }
}
