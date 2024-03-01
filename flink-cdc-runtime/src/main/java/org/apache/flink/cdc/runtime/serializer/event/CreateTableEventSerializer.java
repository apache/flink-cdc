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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link CreateTableEvent}. */
public class CreateTableEventSerializer extends TypeSerializerSingleton<CreateTableEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final CreateTableEventSerializer INSTANCE = new CreateTableEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public CreateTableEvent createInstance() {
        return new CreateTableEvent(TableId.tableId("unknown"), Schema.newBuilder().build());
    }

    @Override
    public CreateTableEvent copy(CreateTableEvent from) {
        return new CreateTableEvent(from.tableId(), schemaSerializer.copy(from.getSchema()));
    }

    @Override
    public CreateTableEvent copy(CreateTableEvent from, CreateTableEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(CreateTableEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        schemaSerializer.serialize(record.getSchema(), target);
    }

    @Override
    public CreateTableEvent deserialize(DataInputView source) throws IOException {
        return new CreateTableEvent(
                tableIdSerializer.deserialize(source), schemaSerializer.deserialize(source));
    }

    @Override
    public CreateTableEvent deserialize(CreateTableEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<CreateTableEvent> snapshotConfiguration() {
        return new CreateTableEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class CreateTableEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<CreateTableEvent> {

        public CreateTableEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
