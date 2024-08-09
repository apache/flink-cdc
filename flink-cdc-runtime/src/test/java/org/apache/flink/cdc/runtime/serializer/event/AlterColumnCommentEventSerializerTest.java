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
import org.apache.flink.cdc.common.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link AlterColumnCommentEventSerializer}. */
public class AlterColumnCommentEventSerializerTest
        extends SerializerTestBase<AlterColumnCommentEvent> {
    @Override
    protected TypeSerializer<AlterColumnCommentEvent> createSerializer() {
        return AlterColumnCommentEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<AlterColumnCommentEvent> getTypeClass() {
        return AlterColumnCommentEvent.class;
    }

    @Override
    protected AlterColumnCommentEvent[] getTestData() {
        Map<String, String> map = new HashMap<>();
        map.put("col1", "Comments of Column One");
        map.put("col2", "Comments of Column Two");
        Map<String, String> oldMap = new HashMap<>();
        oldMap.put("col1", "Old Comments of Column One");
        oldMap.put("col2", "Old Comments of Column Two");
        return new AlterColumnCommentEvent[] {
            new AlterColumnCommentEvent(TableId.tableId("table"), map),
            new AlterColumnCommentEvent(TableId.tableId("schema", "table"), map),
            new AlterColumnCommentEvent(TableId.tableId("namespace", "schema", "table"), map),
            new AlterColumnCommentEvent(TableId.tableId("table"), map, oldMap),
            new AlterColumnCommentEvent(TableId.tableId("schema", "table"), map, oldMap),
            new AlterColumnCommentEvent(
                    TableId.tableId("namespace", "schema", "table"), map, oldMap)
        };
    }
}
