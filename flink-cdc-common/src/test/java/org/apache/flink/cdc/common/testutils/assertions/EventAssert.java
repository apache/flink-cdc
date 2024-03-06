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

package org.apache.flink.cdc.common.testutils.assertions;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import org.assertj.core.api.AbstractAssert;

/** Assertions for {@link EventAssert}. */
public class EventAssert extends AbstractAssert<EventAssert, Event> {
    public static EventAssert assertThatEvent(Event event) {
        return new EventAssert(event);
    }

    protected EventAssert(Event event) {
        super(event, EventAssert.class);
    }

    public DataChangeEventAssert asDataChangeEvent() {
        isInstanceOf(DataChangeEvent.class);
        return DataChangeEventAssert.assertThatDataChangeEvent((DataChangeEvent) actual);
    }

    public SchemaChangeEventAssert asSchemaChangeEvent() {
        isInstanceOf(SchemaChangeEvent.class);
        return SchemaChangeEventAssert.assertThatSchemaChangeEvent((SchemaChangeEvent) actual);
    }
}
