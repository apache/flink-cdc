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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.data.GenericRecordData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DataChangeEvent}. */
class DataChangeEventTest {

    private static final TableId TABLE_ID = TableId.tableId("schema", "table");
    private static final GenericRecordData RECORD = GenericRecordData.of("record");

    @Test
    void testOpTypeString() {
        assertThat(DataChangeEvent.insertEvent(TABLE_ID, RECORD).opTypeString(true))
                .isEqualTo("+I");
        assertThat(DataChangeEvent.updateEvent(TABLE_ID, RECORD, RECORD).opTypeString(false))
                .isEqualTo("-U");
        assertThat(DataChangeEvent.updateEvent(TABLE_ID, RECORD, RECORD).opTypeString(true))
                .isEqualTo("+U");
        assertThat(DataChangeEvent.deleteEvent(TABLE_ID, RECORD).opTypeString(false))
                .isEqualTo("-D");
        assertThat(DataChangeEvent.replaceEvent(TABLE_ID, RECORD).opTypeString(false))
                .isEqualTo("+U");
        assertThat(DataChangeEvent.replaceEvent(TABLE_ID, RECORD).opTypeString(true))
                .isEqualTo("+U");
    }
}
