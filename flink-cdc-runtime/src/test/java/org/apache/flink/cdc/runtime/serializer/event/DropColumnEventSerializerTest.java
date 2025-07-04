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
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;

/** A test for the {@link DropColumnEventSerializer}. */
class DropColumnEventSerializerTest extends SerializerTestBase<DropColumnEvent> {
    @Override
    protected TypeSerializer<DropColumnEvent> createSerializer() {
        return DropColumnEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<DropColumnEvent> getTypeClass() {
        return DropColumnEvent.class;
    }

    @Override
    protected DropColumnEvent[] getTestData() {
        return new DropColumnEvent[] {
            new DropColumnEvent(TableId.tableId("table"), Arrays.asList("c1", "c2")),
            new DropColumnEvent(
                    TableId.tableId("schema", "table"), Arrays.asList("m1", "m2", "m3")),
            new DropColumnEvent(
                    TableId.tableId("namespace", "schema", "table"), Arrays.asList("c1", "c2"))
        };
    }
}
