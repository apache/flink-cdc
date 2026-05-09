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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.paimon.sink.v2.blob.BlobWriteContext;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PaimonWriter#write}. */
@ExtendWith(MockitoExtension.class)
class PaimonWriterTest {

    @Mock private Catalog mockCatalog;

    @Mock private FileStoreTable mockTable;

    @Mock private CoreOptions mockCoreOptions;

    private MockedStatic<FlinkCatalogFactory> mockedFactory;
    private MockedStatic<BlobWriteContext> mockedBlobWriteContext;
    private MockedConstruction<StoreSinkWriteImpl> mockedConstruction;

    private PaimonWriter<Event> writer;

    private final TableId testTableId = TableId.parse("test.table1");
    private final Identifier testIdentifier = Identifier.fromString("test.table1");

    @BeforeEach
    void setUp() throws Exception {
        mockedFactory = Mockito.mockStatic(FlinkCatalogFactory.class);
        mockedBlobWriteContext = Mockito.mockStatic(BlobWriteContext.class);

        mockedFactory
                .when(() -> FlinkCatalogFactory.createPaimonCatalog(any(Options.class)))
                .thenReturn(mockCatalog);

        BlobWriteContext mockBlobContext = BlobWriteContext.empty();
        mockedBlobWriteContext
                .when(() -> BlobWriteContext.fromTable(anyBoolean(), any(FileStoreTable.class)))
                .thenReturn(mockBlobContext);

        when(mockCatalog.getTable(any(Identifier.class))).thenReturn(mockTable);
        when(mockCoreOptions.writeBufferSize()).thenReturn(32 * 1024 * 1024L);
        when(mockCoreOptions.pageSize()).thenReturn(32 * 1024);
        when(mockCoreOptions.bucket()).thenReturn(1);
        when(mockTable.coreOptions()).thenReturn(mockCoreOptions);

        MetricGroup metricGroup = UnregisteredMetricsGroup.createOperatorMetricGroup();
        PaimonRecordEventSerializer serializer =
                new PaimonRecordEventSerializer(ZoneId.systemDefault());

        Options catalogOptions = new Options();

        mockedConstruction =
                Mockito.mockConstruction(
                        StoreSinkWriteImpl.class,
                        (mock, context) ->
                                lenient()
                                        .when(mock.prepareCommit(anyBoolean(), anyLong()))
                                        .thenReturn(java.util.Collections.emptyList()));

        writer =
                new PaimonWriter<>(catalogOptions, metricGroup, "test_commit_user", serializer, 0L);

        injectMockCatalog(writer, mockCatalog);
    }

    private void injectMockCatalog(PaimonWriter<Event> writer, Catalog catalog) throws Exception {
        Field catalogField = PaimonWriter.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        catalogField.set(writer, catalog);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (mockedFactory != null) {
            mockedFactory.close();
        }
        if (mockedBlobWriteContext != null) {
            mockedBlobWriteContext.close();
        }
        if (mockedConstruction != null) {
            mockedConstruction.close();
        }
    }

    @Test
    void testGetTableInvocationCount() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("value", DataTypes.INT())
                        .physicalColumn("path", DataTypes.STRING())
                        .build();

        writer.write(new CreateTableEvent(testTableId, schema), null);

        RowType rowType =
                RowType.of(DataTypes.STRING().notNull(), DataTypes.INT(), DataTypes.STRING());
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        for (int i = 1; i <= 5; i++) {
            writer.write(
                    DataChangeEvent.insertEvent(
                            testTableId,
                            generator.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("id" + i),
                                        i,
                                        BinaryStringData.fromString("path" + i)
                                    })),
                    null);
        }

        verify(mockCatalog, times(1)).getTable(testIdentifier);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("age", DataTypes.INT())));
        AddColumnEvent addColumnEvent = new AddColumnEvent(testTableId, addedColumns);
        writer.write(addColumnEvent, null);

        verify(mockCatalog, times(2)).getTable(testIdentifier);

        RowType rowTypeWithAge =
                RowType.of(
                        DataTypes.STRING().notNull(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.INT());
        BinaryRecordDataGenerator generatorWithAge = new BinaryRecordDataGenerator(rowTypeWithAge);

        for (int i = 6; i <= 10; i++) {
            writer.write(
                    DataChangeEvent.insertEvent(
                            testTableId,
                            generatorWithAge.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("id" + i),
                                        i,
                                        BinaryStringData.fromString("path" + i),
                                        i + 10
                                    })),
                    null);
        }

        verify(mockCatalog, times(2)).getTable(testIdentifier);

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("value", DataTypes.BIGINT());
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(testTableId, typeMapping);
        writer.write(alterColumnTypeEvent, null);

        verify(mockCatalog, times(3)).getTable(testIdentifier);
    }
}
