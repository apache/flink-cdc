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

package com.ververica.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.stream.Stream;

/** A test for the {@link EventSerializer}. */
public class EventSerializerTest extends SerializerTestBase<Event> {
    @Override
    protected TypeSerializer<Event> createSerializer() {
        return EventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Event> getTypeClass() {
        return Event.class;
    }

    @Override
    protected Event[] getTestData() {
        Event[] flushEvents =
                new Event[] {
                    new FlushEvent(TableId.tableId("table")),
                    new FlushEvent(TableId.tableId("schema", "table")),
                    new FlushEvent(TableId.tableId("namespace", "schema", "table"))
                };
        Event[] dataChangeEvents = new DataChangeEventSerializerTest().getTestData();
        Event[] schemaChangeEvents = new SchemaChangeEventSerializerTest().getTestData();
        return Stream.concat(
                        Arrays.stream(flushEvents),
                        Stream.concat(
                                Arrays.stream(schemaChangeEvents), Arrays.stream(dataChangeEvents)))
                .toArray(Event[]::new);
    }
}
