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

package org.apache.flink.cdc.connectors.dws.utils;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the writer-independent DWS result oracle. */
class DwsExpectedStateTest {

    private static final TableId TABLE_ID = TableId.tableId("inventory", "customers");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("tenant", DataTypes.INT().notNull())
                    .physicalColumn("id", DataTypes.VARBINARY(16).notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("tenant", "id")
                    .build();

    @Test
    void shouldReducePrimaryKeyChangesDeletesAndReinserts() {
        DwsExpectedState expected = DwsExpectedState.forSchema(SCHEMA);

        GenericRecordData first = row(1, new byte[] {1}, "old");
        GenericRecordData changedKey = row(1, new byte[] {2}, "changed");
        GenericRecordData rebuiltOldKey = row(1, new byte[] {1}, "rebuilt");

        expected.apply(DataChangeEvent.insertEvent(TABLE_ID, first));
        expected.apply(DataChangeEvent.updateEvent(TABLE_ID, first, changedKey));
        expected.apply(DataChangeEvent.insertEvent(TABLE_ID, rebuiltOldKey));
        expected.apply(DataChangeEvent.deleteEvent(TABLE_ID, changedKey));

        assertThat(expected.rows())
                .containsExactly(DwsExpectedState.Row.of(1, bytes(1), "rebuilt"));
    }

    @Test
    void shouldUseBinaryContentForCompositePrimaryKeys() {
        DwsExpectedState expected = DwsExpectedState.forSchema(SCHEMA);

        expected.apply(DataChangeEvent.insertEvent(TABLE_ID, row(7, new byte[] {3, 4}, "first")));
        expected.apply(
                DataChangeEvent.replaceEvent(TABLE_ID, row(7, new byte[] {3, 4}, "replacement")));

        assertThat(expected.rows())
                .containsExactly(DwsExpectedState.Row.of(7, bytes(3, 4), "replacement"));
    }

    private static GenericRecordData row(int tenant, byte[] id, String name) {
        return GenericRecordData.of(tenant, id, BinaryStringData.fromString(name));
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
