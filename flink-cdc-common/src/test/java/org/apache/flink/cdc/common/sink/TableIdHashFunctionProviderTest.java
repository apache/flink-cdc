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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link TableIdHashFunctionProvider}. */
class TableIdHashFunctionProviderTest {

    private static final TableId TABLE_A = TableId.tableId("namespace", "schema", "table_a");
    private static final TableId TABLE_B = TableId.tableId("namespace", "schema", "table_b");
    private static final TableId TABLE_C = TableId.tableId("other_namespace", "schema", "table_a");

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private final TableIdHashFunctionProvider provider = new TableIdHashFunctionProvider();

    @Test
    void testSameTableSameHash() {
        HashFunction<DataChangeEvent> hashFunction = provider.getHashFunction(TABLE_A, SCHEMA);

        DataChangeEvent event1 = createInsertEvent(TABLE_A, 1, "Alice");
        DataChangeEvent event2 = createInsertEvent(TABLE_A, 2, "Bob");
        DataChangeEvent event3 = createInsertEvent(TABLE_A, 3, "Charlie");

        int hash1 = hashFunction.hashcode(event1);
        int hash2 = hashFunction.hashcode(event2);
        int hash3 = hashFunction.hashcode(event3);

        // All events from the same table should have the same hash
        assertThat(hash1).isEqualTo(hash2);
        assertThat(hash2).isEqualTo(hash3);
    }

    @Test
    void testDifferentTableDifferentHash() {
        HashFunction<DataChangeEvent> hashFunctionA = provider.getHashFunction(TABLE_A, SCHEMA);
        HashFunction<DataChangeEvent> hashFunctionB = provider.getHashFunction(TABLE_B, SCHEMA);

        DataChangeEvent eventA = createInsertEvent(TABLE_A, 1, "Alice");
        DataChangeEvent eventB = createInsertEvent(TABLE_B, 1, "Alice");

        int hashA = hashFunctionA.hashcode(eventA);
        int hashB = hashFunctionB.hashcode(eventB);

        // Events from different tables should have different hashes
        assertThat(hashA).isNotEqualTo(hashB);
    }

    @Test
    void testDifferentNamespaceDifferentHash() {
        HashFunction<DataChangeEvent> hashFunctionA = provider.getHashFunction(TABLE_A, SCHEMA);
        HashFunction<DataChangeEvent> hashFunctionC = provider.getHashFunction(TABLE_C, SCHEMA);

        DataChangeEvent eventA = createInsertEvent(TABLE_A, 1, "Alice");
        DataChangeEvent eventC = createInsertEvent(TABLE_C, 1, "Alice");

        int hashA = hashFunctionA.hashcode(eventA);
        int hashC = hashFunctionC.hashcode(eventC);

        // Events from different namespaces should have different hashes
        assertThat(hashA).isNotEqualTo(hashC);
    }

    @Test
    void testHashIsPositive() {
        HashFunction<DataChangeEvent> hashFunction = provider.getHashFunction(TABLE_A, SCHEMA);

        DataChangeEvent event = createInsertEvent(TABLE_A, 1, "Alice");
        int hash = hashFunction.hashcode(event);

        // Hash should always be non-negative
        assertThat(hash).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testHashIgnoresPrimaryKeyValue() {
        HashFunction<DataChangeEvent> hashFunction = provider.getHashFunction(TABLE_A, SCHEMA);

        // Create events with different primary key values
        DataChangeEvent eventPk1 = createInsertEvent(TABLE_A, 1, "Alice");
        DataChangeEvent eventPk2 = createInsertEvent(TABLE_A, 2, "Bob");
        DataChangeEvent eventPk3 = createInsertEvent(TABLE_A, 100, "Charlie");

        int hash1 = hashFunction.hashcode(eventPk1);
        int hash2 = hashFunction.hashcode(eventPk2);
        int hash3 = hashFunction.hashcode(eventPk3);

        // Hash should be the same regardless of primary key values
        assertThat(hash1).isEqualTo(hash2);
        assertThat(hash2).isEqualTo(hash3);
    }

    @Test
    void testHashIgnoresOperationType() {
        HashFunction<DataChangeEvent> hashFunction = provider.getHashFunction(TABLE_A, SCHEMA);

        DataChangeEvent insertEvent = createInsertEvent(TABLE_A, 1, "Alice");
        DataChangeEvent updateEvent = createUpdateEvent(TABLE_A, 1, "Alice", "Alice_Updated");
        DataChangeEvent deleteEvent = createDeleteEvent(TABLE_A, 1, "Alice");

        int insertHash = hashFunction.hashcode(insertEvent);
        int updateHash = hashFunction.hashcode(updateEvent);
        int deleteHash = hashFunction.hashcode(deleteEvent);

        // Hash should be the same regardless of operation type
        assertThat(insertHash).isEqualTo(updateHash);
        assertThat(updateHash).isEqualTo(deleteHash);
    }

    private DataChangeEvent createInsertEvent(TableId tableId, int id, String name) {
        return DataChangeEvent.insertEvent(
                tableId, GenericRecordData.of(id, new BinaryStringData(name)));
    }

    private DataChangeEvent createUpdateEvent(
            TableId tableId, int id, String oldName, String newName) {
        return DataChangeEvent.updateEvent(
                tableId,
                GenericRecordData.of(id, new BinaryStringData(oldName)),
                GenericRecordData.of(id, new BinaryStringData(newName)));
    }

    private DataChangeEvent createDeleteEvent(TableId tableId, int id, String name) {
        return DataChangeEvent.deleteEvent(
                tableId, GenericRecordData.of(id, new BinaryStringData(name)));
    }
}
