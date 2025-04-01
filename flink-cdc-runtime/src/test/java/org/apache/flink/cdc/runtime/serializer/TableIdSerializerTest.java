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

package org.apache.flink.cdc.runtime.serializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.TableId;

/** A test for the {@link TableIdSerializer}. */
class TableIdSerializerTest extends SerializerTestBase<TableId> {
    @Override
    protected TypeSerializer<TableId> createSerializer() {
        return TableIdSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<TableId> getTypeClass() {
        return TableId.class;
    }

    @Override
    protected TableId[] getTestData() {
        return new TableId[] {
            TableId.tableId("table"),
            TableId.tableId("schema", "table"),
            TableId.tableId("namespace", "schema", "table")
        };
    }
}
