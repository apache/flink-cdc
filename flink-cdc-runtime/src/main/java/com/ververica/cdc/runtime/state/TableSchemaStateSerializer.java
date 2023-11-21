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

package com.ververica.cdc.runtime.state;

import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.runtime.serializer.TableIdSerializer;
import com.ververica.cdc.runtime.serializer.schema.SchemaSerializer;

import java.io.IOException;

/** A serializer for the {@link TableSchemaState}, use in {@link StatefulSink}. */
public class TableSchemaStateSerializer implements SimpleVersionedSerializer<TableSchemaState> {

    /** the default version for deserialize method. */
    public static final int DEFAULT_VERSION = 0;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    @Override
    public int getVersion() {
        return DEFAULT_VERSION;
    }

    @Override
    public byte[] serialize(TableSchemaState state) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        TableIdSerializer.INSTANCE.serialize(state.getTableId(), out);
        SchemaSerializer.INSTANCE.serialize(state.getSchema(), out);
        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        state.serializedFormCache = result;
        return result;
    }

    @Override
    public TableSchemaState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        TableSchemaState event =
                new TableSchemaState(
                        TableIdSerializer.INSTANCE.deserialize(in),
                        SchemaSerializer.INSTANCE.deserialize(in));
        in.releaseArrays();
        return event;
    }
}
