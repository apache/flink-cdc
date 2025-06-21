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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** A test for {@link IcebergSink}. */
public class IcebergSinkITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private static StreamExecutionEnvironment env;

    private static Catalog catalog;

    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_TIMEOUT_MS = 30000;

    private static final Map<String, String> catalogOptions = new HashMap<>();

    private static final TableId tableId = TableId.tableId("test_db", "test_table");

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
    }

    @Test
    public void testIcebergSink() throws Exception {
        List<Event> events = new ArrayList<>();
        events.addAll(generateEvents(tableId));
        events.addAll(generateEvents(TableId.tableId("test_db", "test_table_2")));
        events.addAll(generateEvents(tableId));
        DataStream<Event> stream = env.fromData(events, TypeInformation.of(Event.class));

        Sink<Event> icebergSink =
                new IcebergSink(catalogOptions, null, null, CompactionOptions.builder().build());
        String[] expected = new String[] {"21, 1.732, Disenchanted", "17, 6.28, Doris Day"};
        stream.sinkTo(icebergSink);
        env.execute("Values to Iceberg Sink");

        long startTime = System.currentTimeMillis();
        List<String> actual = new ArrayList<>();
        while (actual.size() != expected.length) {
            if (System.currentTimeMillis() - startTime > DEFAULT_TIMEOUT_MS) {
                throw new RuntimeException("Timeout waiting for table to be populated");
            }
            Thread.sleep(1000);
            actual = fetchTableContent(catalog, tableId);
        }
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        expected = new String[] {"21, 1.732, Disenchanted", "17, 6.28, Doris Day"};
        actual = new ArrayList<>();
        while (actual.size() != expected.length) {
            if (System.currentTimeMillis() - startTime > DEFAULT_TIMEOUT_MS) {
                throw new RuntimeException("Timeout waiting for table to be populated");
            }
            Thread.sleep(1000);
            actual = fetchTableContent(catalog, TableId.tableId("test_db", "test_table_2"));
        }
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    private List<Event> generateEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        icebergMetadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.VARCHAR(17)));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {17, 3.14, BinaryStringData.fromString("Doris Day")})),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19, 2.718, BinaryStringData.fromString("Que Sera Sera")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    21, 1.732, BinaryStringData.fromString("Disenchanted")
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {17, 3.14, BinaryStringData.fromString("Doris Day")}),
                        generator.generate(
                                new Object[] {17, 6.28, BinaryStringData.fromString("Doris Day")})),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19, 2.718, BinaryStringData.fromString("Que Sera Sera")
                                })));
    }

    private List<String> fetchTableContent(Catalog catalog, TableId tableId) {
        List<String> results = new ArrayList<>();
        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        org.apache.iceberg.Schema schema = table.schema();
        CloseableIterable<Record> records = IcebergGenerics.read(table).project(schema).build();
        for (Record record : records) {
            List<String> fieldValues = new ArrayList<>();
            for (Types.NestedField field : schema.columns()) {
                String fieldValue = Objects.toString(record.getField(field.name()), "null");
                fieldValues.add(fieldValue);
            }
            String joinedString = String.join(", ", fieldValues);
            results.add(joinedString);
        }
        return results;
    }
}
