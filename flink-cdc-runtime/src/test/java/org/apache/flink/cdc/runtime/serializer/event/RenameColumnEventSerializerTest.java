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
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link RenameColumnEventSerializer}. */
class RenameColumnEventSerializerTest extends SerializerTestBase<RenameColumnEvent> {
    @Override
    protected TypeSerializer<RenameColumnEvent> createSerializer() {
        return RenameColumnEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<RenameColumnEvent> getTypeClass() {
        return RenameColumnEvent.class;
    }

    @Override
    protected RenameColumnEvent[] getTestData() {
        Map<String, String> map = new HashMap<>();
        map.put("c1", "c2");
        return new RenameColumnEvent[] {
            new RenameColumnEvent(TableId.tableId("table"), map),
            new RenameColumnEvent(TableId.tableId("schema", "table"), map),
            new RenameColumnEvent(TableId.tableId("namespace", "schema", "table"), map)
        };
    }
}
