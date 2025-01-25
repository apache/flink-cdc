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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** A test for {@link IcebergWriter}. */
public class IcebergWriterTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testWrite() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        IcebergWriter icebergWriter =
                new IcebergWriter(catalogOptions, 1, 1, ZoneId.systemDefault());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("test.iceberg_table");

        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "name",
                                        DataTypes.VARCHAR(255).notNull(),
                                        "column for name",
                                        "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "column for age", "91")
                                .physicalColumn(
                                        "description",
                                        DataTypes.STRING(),
                                        "column for descriptions",
                                        "not important")
                                .primaryKey("id")
                                .partitionKey("id", "name")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordData =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("Mark"),
                            10,
                            BinaryStringData.fromString("test")
                        });
        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        icebergWriter.write(dataChangeEvent, null);
        RecordData recordData2 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            2L,
                            BinaryStringData.fromString("Bob"),
                            10,
                            BinaryStringData.fromString("test")
                        });
        DataChangeEvent dataChangeEvent2 = DataChangeEvent.insertEvent(tableId, recordData2);
        icebergWriter.write(dataChangeEvent2, null);
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions);
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);
        CloseableIterable<Record> records =
                IcebergGenerics.read(
                                catalog.loadTable(
                                        TableIdentifier.of(
                                                tableId.getSchemaName(), tableId.getTableName())))
                        .build();
        List<String> result = new ArrayList<>();
        for (Record record : records) {
            result.add(record.toString());
        }
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder("Record(1, Mark, 10, test)", "Record(2, Bob, 10, test)");

        // Add column.
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "newStringColumn",
                                                DataTypes.STRING(),
                                                "comment for newStringColumn",
                                                "not important"))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        icebergWriter.write(addColumnEvent, null);
        binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        SchemaUtils.applySchemaChangeEvent(
                                        createTableEvent.getSchema(), addColumnEvent)
                                .getColumnDataTypes()
                                .toArray(new DataType[0]));
        recordData =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            3L,
                            BinaryStringData.fromString("Mark"),
                            10,
                            BinaryStringData.fromString("test"),
                            BinaryStringData.fromString("newStringColumn"),
                        });
        dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        icebergWriter.write(dataChangeEvent, null);
        writeResults = icebergWriter.prepareCommit();
        collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);
        records =
                IcebergGenerics.read(
                                catalog.loadTable(
                                        TableIdentifier.of(
                                                tableId.getSchemaName(), tableId.getTableName())))
                        .build();
        result = new ArrayList<>();
        for (Record record : records) {
            result.add(record.toString());
        }
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        "Record(1, Mark, 10, test, null)",
                        "Record(2, Bob, 10, test, null)",
                        "Record(3, Mark, 10, test, newStringColumn)");
    }

    private static class MockCommitRequestImpl<CommT> extends CommitRequestImpl<CommT> {

        protected MockCommitRequestImpl(CommT committable) {
            super(
                    committable,
                    InternalSinkCommitterMetricGroup.wrap(
                            UnregisteredMetricsGroup.createOperatorMetricGroup()));
        }
    }
}
