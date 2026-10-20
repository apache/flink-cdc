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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests target-table delete capabilities and their per-table factory cache. */
class IcebergWriterDeleteCapabilityTest {

    @TempDir private Path temporaryFolder;

    private final BinaryRecordDataGenerator generator =
            new BinaryRecordDataGenerator(
                    schema(false, false).getColumnDataTypes().toArray(new DataType[0]));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMixedTableDeleteCapabilities(boolean keyedTableFirst) throws Exception {
        TableId keyedTable = TableId.parse("test.keyed_table");
        TableId appendTable = TableId.parse("test.append_table");
        List<TableId> writeOrder =
                keyedTableFirst
                        ? Arrays.asList(keyedTable, appendTable)
                        : Arrays.asList(appendTable, keyedTable);
        RecordData north = record(1L, "north");
        RecordData east = record(1L, "east");

        try (TestContext context = new TestContext()) {
            Schema keyedSchema = schema(true, true);
            Schema appendSchema = schema(false, true);
            context.createTable(keyedTable, keyedSchema, keyedSchema);
            context.createTable(appendTable, appendSchema, appendSchema);
            for (TableId tableId : writeOrder) {
                context.writer.write(DataChangeEvent.insertEvent(tableId, north), null);
            }
            context.commit();

            // Rebuild both factories after the checkpoint, interleaving tables with opposite
            // capabilities. The keyed row must move partitions; the append table keeps both.
            for (TableId tableId : writeOrder) {
                context.writer.write(DataChangeEvent.updateEvent(tableId, north, east), null);
            }
            context.commit();
            assertThat(context.read(keyedTable)).containsExactly("1, east");
            assertThat(context.read(appendTable)).containsExactlyInAnyOrder("1, north", "1, east");

            for (TableId tableId : writeOrder) {
                context.writer.write(DataChangeEvent.deleteEvent(tableId, east), null);
            }
            context.commit();
            assertThat(context.read(keyedTable)).isEmpty();
            assertThat(context.read(appendTable)).containsExactlyInAnyOrder("1, north", "1, east");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExistingKeyedTargetWithoutCdcPrimaryKey(boolean partitioned) throws Exception {
        TableId tableId = TableId.parse("test.existing_keyed_table");
        RecordData north = record(1L, "north");
        RecordData east = record(1L, "east");

        try (TestContext context = new TestContext()) {
            Schema targetSchema = schema(true, partitioned);
            Schema cdcSchema = schema(false, partitioned);
            assertThat(cdcSchema.primaryKeys()).isEmpty();
            context.createTable(tableId, targetSchema, cdcSchema);
            assertThat(
                            context.catalog
                                    .loadTable(TableIdentifier.parse(tableId.identifier()))
                                    .schema()
                                    .identifierFieldIds())
                    .isNotEmpty();

            context.writer.write(DataChangeEvent.insertEvent(tableId, north), null);
            context.commit();
            context.writer.write(DataChangeEvent.updateEvent(tableId, north, east), null);
            context.commit();
            assertThat(context.read(tableId)).containsExactly("1, east");

            // A source-schema PK check would incorrectly ignore this delete.
            context.writer.write(DataChangeEvent.deleteEvent(tableId, east), null);
            context.commit();
            assertThat(context.read(tableId)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCatalogLoadsOnlyOnFactoryCacheMiss(boolean hasPrimaryKey) throws Exception {
        TableId tableId = TableId.parse("test.cached_table");
        int rowCount = 20;

        try (TestContext context = new TestContext()) {
            Schema schema = schema(hasPrimaryKey, false);
            context.createTable(tableId, schema, schema);
            AtomicInteger loadTableCalls = countWriterCatalogLoads(context.writer);

            for (long id = 1; id <= rowCount; id++) {
                context.writer.write(
                        DataChangeEvent.insertEvent(tableId, record(id, "north")), null);
                context.writer.write(
                        DataChangeEvent.updateEvent(
                                tableId, record(id, "north"), record(id, "east")),
                        null);
            }
            assertThat(loadTableCalls.get()).isEqualTo(1);

            // flush invalidates factories, not active writers, and does not load eagerly.
            context.writer.flush(false);
            assertThat(loadTableCalls.get()).isEqualTo(1);
            for (long id = 1; id <= rowCount; id++) {
                context.writer.write(
                        DataChangeEvent.updateEvent(
                                tableId, record(id, "east"), record(id, "south")),
                        null);
            }
            assertThat(loadTableCalls.get()).isEqualTo(2);
            context.commit();
            assertThat(loadTableCalls.get()).isEqualTo(2);

            // Checkpoint invalidation causes exactly one new load, even for a delete-only batch.
            for (long id = 1; id <= rowCount; id++) {
                context.writer.write(
                        DataChangeEvent.deleteEvent(tableId, record(id, "south")), null);
            }
            assertThat(loadTableCalls.get()).isEqualTo(3);
            context.commit();
            assertThat(loadTableCalls.get()).isEqualTo(3);
            assertThat(context.read(tableId)).hasSize(hasPrimaryKey ? 0 : rowCount * 3);
        }
    }

    private static AtomicInteger countWriterCatalogLoads(IcebergWriter writer) throws Exception {
        Field catalogField = IcebergWriter.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        Catalog delegate = (Catalog) catalogField.get(writer);
        AtomicInteger loadTableCalls = new AtomicInteger();
        Catalog countingCatalog =
                (Catalog)
                        Proxy.newProxyInstance(
                                Catalog.class.getClassLoader(),
                                new Class<?>[] {Catalog.class},
                                (proxy, method, args) -> {
                                    if ("loadTable".equals(method.getName())) {
                                        loadTableCalls.incrementAndGet();
                                    }
                                    try {
                                        return method.invoke(delegate, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
        catalogField.set(writer, countingCatalog);
        return loadTableCalls;
    }

    private static Schema schema(boolean hasPrimaryKey, boolean partitioned) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("region", DataTypes.STRING().notNull());
        if (hasPrimaryKey) {
            builder.primaryKey("id");
        }
        if (partitioned) {
            builder.partitionKey("region");
        }
        return builder.build();
    }

    private RecordData record(long id, String region) {
        return generator.generate(new Object[] {id, BinaryStringData.fromString(region)});
    }

    private class TestContext implements AutoCloseable {

        private final Catalog catalog;

        private final IcebergMetadataApplier metadataApplier;

        private final IcebergWriter writer;

        private final IcebergCommitter committer;

        private TestContext() {
            Map<String, String> catalogOptions = new HashMap<>();
            catalogOptions.put("type", "hadoop");
            catalogOptions.put(
                    "warehouse", temporaryFolder.resolve(UUID.randomUUID().toString()).toString());
            catalogOptions.put("cache-enabled", "false");
            catalog =
                    CatalogUtil.buildIcebergCatalog(
                            "cdc-iceberg-catalog", catalogOptions, new Configuration());
            metadataApplier = new IcebergMetadataApplier(catalogOptions);
            writer =
                    new IcebergWriter(
                            catalogOptions,
                            1,
                            1,
                            ZoneId.of("UTC"),
                            0,
                            UUID.randomUUID().toString(),
                            UUID.randomUUID().toString(),
                            new HashMap<>());
            committer = new IcebergCommitter(catalogOptions, new HashMap<>());
        }

        private void createTable(TableId tableId, Schema targetSchema, Schema cdcSchema) {
            metadataApplier.applySchemaChange(new CreateTableEvent(tableId, targetSchema));
            try {
                writer.write(new CreateTableEvent(tableId, cdcSchema), null);
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void commit() throws Exception {
            committer.commit(
                    writer.prepareCommit().stream()
                            .map(IcebergWriterTest.MockCommitRequestImpl::new)
                            .collect(Collectors.toList()));
        }

        private List<String> read(TableId tableId) throws Exception {
            List<String> result = new ArrayList<>();
            try (CloseableIterable<Record> records =
                    IcebergGenerics.read(
                                    catalog.loadTable(TableIdentifier.parse(tableId.identifier())))
                            .build()) {
                for (Record record : records) {
                    result.add(record.getField("id") + ", " + record.getField("region"));
                }
            }
            return result;
        }

        @Override
        public void close() throws Exception {
            writer.close();
            committer.close();
            if (catalog instanceof AutoCloseable) {
                ((AutoCloseable) catalog).close();
            }
        }
    }
}
