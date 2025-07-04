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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

/** A test for the {@link EventSerializer}. */
class EventSerializerTest extends SerializerTestBase<Event> {
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
                    new FlushEvent(1, Collections.emptyList(), SchemaChangeEventType.CREATE_TABLE),
                    new FlushEvent(
                            2,
                            Collections.singletonList(TableId.tableId("schema", "table")),
                            SchemaChangeEventType.CREATE_TABLE),
                    new FlushEvent(
                            3,
                            Arrays.asList(
                                    TableId.tableId("schema", "table"),
                                    TableId.tableId("namespace", "schema", "table")),
                            SchemaChangeEventType.CREATE_TABLE)
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
