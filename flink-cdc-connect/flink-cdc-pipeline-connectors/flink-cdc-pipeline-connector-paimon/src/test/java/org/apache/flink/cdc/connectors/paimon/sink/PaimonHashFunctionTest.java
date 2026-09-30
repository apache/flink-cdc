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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonHashFunction}. */
class PaimonHashFunctionTest {

    private static final String TEST_DATABASE = "test";

    @Test
    public void testHashCodeForAppendOnlyTable() throws IOException {
        TableId tableId = TableId.tableId(TEST_DATABASE, "test_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("pt", DataTypes.STRING())
                        .physicalColumn("variantCol", DataTypes.VARIANT())
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        // No primary keys: append-only table. No catalog access required.
        PaimonHashFunction hashFunction = new PaimonHashFunction(schema, ZoneId.systemDefault(), 4);
        DataChangeEvent dataChangeEvent1 =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024"),
                                    BinaryVariantInternalBuilder.parseJson(
                                            "{\"a\":1,\"b\":\"hello\",\"c\":3.1}", false)
                                }));
        int key1 = hashFunction.hashcode(dataChangeEvent1);

        DataChangeEvent dataChangeEvent2 =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024"),
                                    BinaryVariantInternalBuilder.parseJson(
                                            "{\"a\":1,\"b\":\"hello\",\"c\":3.1}", false)
                                }));
        int key2 = hashFunction.hashcode(dataChangeEvent2);

        DataChangeEvent dataChangeEvent3 =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024"),
                                    BinaryVariantInternalBuilder.parseJson(
                                            "{\"a\":1,\"b\":\"hello\",\"c\":3.1}", false)
                                }));
        int key3 = hashFunction.hashcode(dataChangeEvent3);
        assertThat(key1).isBetween(0, 3);
        assertThat(key2).isBetween(0, 3);
        assertThat(key3).isBetween(0, 3);
    }

    @Test
    void testHashCodeForFixedBucketTable() {
        TableId tableId = TableId.tableId(TEST_DATABASE, "test_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("pt", DataTypes.STRING())
                        .primaryKey("col1", "pt")
                        .partitionKey("pt")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        // Primary keys present: table-aware hashing. No catalog access required.
        PaimonHashFunction hashFunction = new PaimonHashFunction(schema, ZoneId.systemDefault(), 4);
        DataChangeEvent dataChangeEvent1 =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024")
                                }));
        int key1 = hashFunction.hashcode(dataChangeEvent1);

        DataChangeEvent dataChangeEvent2 =
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2024")
                                }));
        int key2 = hashFunction.hashcode(dataChangeEvent2);

        DataChangeEvent dataChangeEvent3 =
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2024")
                                }));
        int key3 = hashFunction.hashcode(dataChangeEvent3);

        assertThat(key1).isEqualTo(key2).isEqualTo(key3);
    }
}
