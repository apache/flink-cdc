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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOperator;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Tests for {@link CompactionOperator}. */
public class CompactionOperatorTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testCompationOperator() throws IOException, InterruptedException {
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
                                .physicalColumn(
                                        "bool_column",
                                        DataTypes.BOOLEAN(),
                                        "column for bool",
                                        "false")
                                .physicalColumn(
                                        "float_column",
                                        DataTypes.FLOAT(),
                                        "column for float",
                                        "1.0")
                                .physicalColumn(
                                        "double_column",
                                        DataTypes.DOUBLE(),
                                        "column for double",
                                        "1.0")
                                .physicalColumn(
                                        "decimal_column",
                                        DataTypes.DECIMAL(10, 2),
                                        "column for decimal",
                                        "1.0")
                                .physicalColumn(
                                        "date_column", DataTypes.DATE(), "column for date", "12345")
                                .primaryKey("id")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions);
        // Commit many times.
        int smallFileCount = 100;
        for (long i = 0; i < smallFileCount; i++) {
            RecordData recordData =
                    binaryRecordDataGenerator.generate(
                            new Object[] {
                                i,
                                BinaryStringData.fromString("Mark"),
                                10,
                                BinaryStringData.fromString("test"),
                                true,
                                1.0f,
                                1.0d,
                                DecimalData.fromBigDecimal(new BigDecimal(1.0), 10, 2),
                                DateData.fromEpochDay(9)
                            });
            icebergWriter.write(DataChangeEvent.insertEvent(tableId, recordData), null);
            Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                    icebergWriter.prepareCommit().stream()
                            .map(IcebergWriterTest.MockCommitRequestImpl::new)
                            .collect(Collectors.toList());
            icebergCommitter.commit(collection);
        }
        CompactionOperator compactionOperator =
                new CompactionOperator(
                        catalogOptions, CompactionOptions.builder().commitInterval(1).build());
        compactionOperator.processElement(
                new StreamRecord<>(
                        new CommittableWithLineage<>(
                                new WriteResultWrapper(null, tableId), 0L, 0)));
        Map<String, String> summary =
                catalog.loadTable(TableIdentifier.parse(tableId.identifier()))
                        .currentSnapshot()
                        .summary();
        Assertions.assertThat(summary.get("deleted-data-files"))
                .isEqualTo(String.valueOf(smallFileCount));
    }
}
