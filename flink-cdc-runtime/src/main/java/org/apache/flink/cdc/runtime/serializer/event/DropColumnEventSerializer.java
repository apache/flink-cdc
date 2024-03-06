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
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link DropColumnEvent}. */
public class DropColumnEventSerializer extends TypeSerializerSingleton<DropColumnEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DropColumnEventSerializer INSTANCE = new DropColumnEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final ListSerializer<String> columnNamesSerializer =
            new ListSerializer<>(StringSerializer.INSTANCE);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public DropColumnEvent createInstance() {
        return new DropColumnEvent(TableId.tableId("unknown"), Collections.emptyList());
    }

    @Override
    public DropColumnEvent copy(DropColumnEvent from) {
        return new DropColumnEvent(
                from.tableId(), columnNamesSerializer.copy(from.getDroppedColumnNames()));
    }

    @Override
    public DropColumnEvent copy(DropColumnEvent from, DropColumnEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DropColumnEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        columnNamesSerializer.serialize(record.getDroppedColumnNames(), target);
    }

    @Override
    public DropColumnEvent deserialize(DataInputView source) throws IOException {
        return new DropColumnEvent(
                tableIdSerializer.deserialize(source), columnNamesSerializer.deserialize(source));
    }

    @Override
    public DropColumnEvent deserialize(DropColumnEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<DropColumnEvent> snapshotConfiguration() {
        return new DropColumnEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DropColumnEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DropColumnEvent> {

        public DropColumnEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
