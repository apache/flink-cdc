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

package org.apache.flink.cdc.connectors.paimon.sink.v2.bucket;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.ListSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializerSingleton} for {@link BucketWrapperChangeEvent}. */
public class BucketWrapperEventSerializer extends TypeSerializerSingleton<Event> {

    private static final long serialVersionUID = 1L;

    private final EnumSerializer<EventClass> enumSerializer =
            new EnumSerializer<>(EventClass.class);

    private final EventSerializer eventSerializer = EventSerializer.INSTANCE;

    private final ListSerializer<TableId> tableIdListSerializer =
            new ListSerializer<>(TableIdSerializer.INSTANCE);
    private final EnumSerializer<SchemaChangeEventType> schemaChangeEventTypeEnumSerializer =
            new EnumSerializer<>(SchemaChangeEventType.class);
    /** Sharable instance of the TableIdSerializer. */
    public static final BucketWrapperEventSerializer INSTANCE = new BucketWrapperEventSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Event createInstance() {
        return new Event() {};
    }

    @Override
    public Event copy(Event event) {
        return event;
    }

    @Override
    public Event copy(Event from, Event reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(Event event, DataOutputView dataOutputView) throws IOException {
        if (event instanceof BucketWrapperChangeEvent) {
            BucketWrapperChangeEvent bucketWrapperChangeEvent = (BucketWrapperChangeEvent) event;
            enumSerializer.serialize(EventClass.BUCKET_WRAPPER_CHANGE_EVENT, dataOutputView);
            dataOutputView.writeInt(bucketWrapperChangeEvent.getBucket());
            eventSerializer.serialize(bucketWrapperChangeEvent.getInnerEvent(), dataOutputView);
        } else if (event instanceof BucketWrapperFlushEvent) {
            enumSerializer.serialize(EventClass.BUCKET_WRAPPER_FLUSH_EVENT, dataOutputView);
            BucketWrapperFlushEvent bucketWrapperFlushEvent = (BucketWrapperFlushEvent) event;
            dataOutputView.writeInt(bucketWrapperFlushEvent.getBucket());
            dataOutputView.writeInt(bucketWrapperFlushEvent.getSourceSubTaskId());
            dataOutputView.writeInt(bucketWrapperFlushEvent.getBucketAssignTaskId());
            tableIdListSerializer.serialize(bucketWrapperFlushEvent.getTableIds(), dataOutputView);
            schemaChangeEventTypeEnumSerializer.serialize(
                    bucketWrapperFlushEvent.getSchemaChangeEventType(), dataOutputView);
        }
    }

    @Override
    public Event deserialize(DataInputView source) throws IOException {
        EventClass eventClass = enumSerializer.deserialize(source);
        if (eventClass.equals(EventClass.BUCKET_WRAPPER_FLUSH_EVENT)) {
            return new BucketWrapperFlushEvent(
                    source.readInt(),
                    source.readInt(),
                    source.readInt(),
                    tableIdListSerializer.deserialize(source),
                    schemaChangeEventTypeEnumSerializer.deserialize(source));
        } else {
            return new BucketWrapperChangeEvent(
                    source.readInt(), (ChangeEvent) eventSerializer.deserialize(source));
        }
    }

    @Override
    public Event deserialize(Event reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Event> snapshotConfiguration() {
        return new EventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class EventSerializerSnapshot extends SimpleTypeSerializerSnapshot<Event> {

        public EventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }

    enum EventClass {
        BUCKET_WRAPPER_CHANGE_EVENT,
        BUCKET_WRAPPER_FLUSH_EVENT
    }
}
