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

import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.HashMap;
import java.util.Map;

/** A test for the {@link AlterColumnTypeEventSerializer}. */
public class AlterColumnTypeEventSerializerTest extends SerializerTestBase<AlterColumnTypeEvent> {
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
        return new AlterColumnTypeEvent[] {
            new AlterColumnTypeEvent(TableId.tableId("table"), map),
            new AlterColumnTypeEvent(TableId.tableId("schema", "table"), map),
            new AlterColumnTypeEvent(TableId.tableId("namespace", "schema", "table"), map)
        };
    }
}
