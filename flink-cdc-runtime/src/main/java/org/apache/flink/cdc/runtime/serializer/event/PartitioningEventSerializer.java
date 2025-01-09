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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link PartitioningEvent}. */
@Internal
public class PartitioningEventSerializer extends TypeSerializerSingleton<PartitioningEvent> {

    public static final PartitioningEventSerializer INSTANCE = new PartitioningEventSerializer();

    private final EventSerializer eventSerializer = EventSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public PartitioningEvent createInstance() {
        return PartitioningEvent.ofDistributed(null, -1, -1);
    }

    @Override
    public PartitioningEvent copy(PartitioningEvent from) {
        return PartitioningEvent.ofDistributed(
                eventSerializer.copy(from.getPayload()),
                from.getSourcePartition(),
                from.getTargetPartition());
    }

    @Override
    public PartitioningEvent copy(PartitioningEvent from, PartitioningEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(PartitioningEvent record, DataOutputView target) throws IOException {
        eventSerializer.serialize(record.getPayload(), target);
        target.writeInt(record.getSourcePartition());
        target.writeInt(record.getTargetPartition());
    }

    @Override
    public PartitioningEvent deserialize(DataInputView source) throws IOException {
        Event payload = eventSerializer.deserialize(source);
        int sourcePartition = source.readInt();
        int targetPartition = source.readInt();
        return PartitioningEvent.ofDistributed(payload, sourcePartition, targetPartition);
    }

    @Override
    public PartitioningEvent deserialize(PartitioningEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        PartitioningEvent deserialized = deserialize(source);
        serialize(deserialized, target);
    }

    @Override
    public TypeSerializerSnapshot<PartitioningEvent> snapshotConfiguration() {
        return new PartitioningEventSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link PartitioningEventSerializer}. */
    public static final class PartitioningEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<PartitioningEvent> {

        public PartitioningEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
