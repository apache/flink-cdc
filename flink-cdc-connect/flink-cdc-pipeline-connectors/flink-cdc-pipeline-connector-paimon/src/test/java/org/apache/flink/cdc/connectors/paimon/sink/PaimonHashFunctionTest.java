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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonHashFunction}. */
class PaimonHashFunctionTest {

    @TempDir public static Path temporaryFolder;

    private Catalog catalog;

    private Options catalogOptions;

    private static final String TEST_DATABASE = "test";

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        catalogOptions = new Options();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.setString("warehouse", warehouse);
        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        catalog.createDatabase(TEST_DATABASE, true);
    }

    @AfterEach
    public void afterEach() throws Exception {
        catalog.dropDatabase(TEST_DATABASE, true, true);
        catalog.close();
    }

    @Test
    public void testHashCodeForAppendOnlyTable() {
        TableId tableId = TableId.tableId(TEST_DATABASE, "test_table");
        Map<String, String> tableOptions = new HashMap<>();
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("pt", DataTypes.STRING())
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        PaimonHashFunction hashFunction =
                new PaimonHashFunction(catalogOptions, tableId, schema, ZoneId.systemDefault(), 4);
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
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024")
                                }));
        int key2 = hashFunction.hashcode(dataChangeEvent2);

        DataChangeEvent dataChangeEvent3 =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2024")
                                }));
        int key3 = hashFunction.hashcode(dataChangeEvent3);
        assertThat(key1).isBetween(0, 3);
        assertThat(key2).isBetween(0, 3);
        assertThat(key3).isBetween(0, 3);
    }

    @Test
    void testHashCodeForFixedBucketTable() {
        TableId tableId = TableId.tableId(TEST_DATABASE, "test_table");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "10");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("pt", DataTypes.STRING())
                        .primaryKey("col1", "pt")
                        .partitionKey("pt")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        PaimonHashFunction hashFunction =
                new PaimonHashFunction(catalogOptions, tableId, schema, ZoneId.systemDefault(), 4);
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
