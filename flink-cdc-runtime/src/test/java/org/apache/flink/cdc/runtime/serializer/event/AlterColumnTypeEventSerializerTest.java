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
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link AlterColumnTypeEventSerializer}. */
class AlterColumnTypeEventSerializerTest extends SerializerTestBase<AlterColumnTypeEvent> {
    @Override
    protected TypeSerializer<AlterColumnTypeEvent> createSerializer() {
        return AlterColumnTypeEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<AlterColumnTypeEvent> getTypeClass() {
        return AlterColumnTypeEvent.class;
    }

    @Override
    protected AlterColumnTypeEvent[] getTestData() {
        Map<String, DataType> map = new HashMap<>();
        map.put("col1", DataTypes.BYTES());
        map.put("col2", DataTypes.TIME());

        Map<String, DataType> oldMap = new HashMap<>();
        oldMap.put("col1", DataTypes.TIME());
        oldMap.put("col2", DataTypes.BYTES());
        return new AlterColumnTypeEvent[] {
            new AlterColumnTypeEvent(TableId.tableId("table"), map),
            new AlterColumnTypeEvent(TableId.tableId("schema", "table"), map),
            new AlterColumnTypeEvent(TableId.tableId("namespace", "schema", "table"), map),
            new AlterColumnTypeEvent(TableId.tableId("table"), map, oldMap),
            new AlterColumnTypeEvent(TableId.tableId("schema", "table"), map, oldMap),
            new AlterColumnTypeEvent(TableId.tableId("namespace", "schema", "table"), map, oldMap)
        };
    }
}
