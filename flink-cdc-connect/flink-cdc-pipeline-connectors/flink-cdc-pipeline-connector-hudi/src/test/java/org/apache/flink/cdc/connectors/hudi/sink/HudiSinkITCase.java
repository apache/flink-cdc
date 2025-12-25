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

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.hudi.sink.v2.HudiSink;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.StreamerUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_PROPERTIES_FILE;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.configuration.FlinkOptions.BUCKET_INDEX_NUM_BUCKETS;
import static org.apache.hudi.configuration.FlinkOptions.INDEX_KEY_FIELD;
import static org.apache.hudi.configuration.FlinkOptions.INDEX_TYPE;
import static org.apache.hudi.configuration.FlinkOptions.ORDERING_FIELDS;
import static org.apache.hudi.configuration.FlinkOptions.PARTITION_PATH_FIELD;
import static org.apache.hudi.configuration.FlinkOptions.PATH;
import static org.apache.hudi.configuration.FlinkOptions.RECORD_KEY_FIELD;
import static org.apache.hudi.configuration.FlinkOptions.TABLE_NAME;
import static org.apache.hudi.configuration.FlinkOptions.TABLE_TYPE;
import static org.apache.hudi.configuration.FlinkOptions.WRITE_TASKS;

/** A test for {@link HudiSink}. */
public class HudiSinkITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private static StreamExecutionEnvironment env;

    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_TIMEOUT_MS = 60000;

    private static final TableId tableId = TableId.tableId("test_db", "test_table");

    private static final org.apache.flink.cdc.common.configuration.Configuration catalogConf =
            new org.apache.flink.cdc.common.configuration.Configuration();

    @BeforeAll
    public static void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Files.createDirectory(Paths.get(warehouse));
        catalogConf.set(HudiConfig.PATH, warehouse);
        catalogConf.set(HudiConfig.TABLE_TYPE, "MERGE_ON_READ");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHudiSink(boolean partitioned) throws Exception {
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Configuration conf = new Configuration();
        conf.set(PATH, warehouse);
        conf.set(TABLE_NAME, "test_table");
        conf.set(TABLE_TYPE, "MERGE_ON_READ");
        conf.set(RECORD_KEY_FIELD, "id");
        conf.set(ORDERING_FIELDS, "ts");
        conf.set(INDEX_TYPE, "BUCKET");
        conf.set(INDEX_KEY_FIELD, "id");
        conf.set(BUCKET_INDEX_NUM_BUCKETS, 2);
        conf.set(WRITE_TASKS, 2);
        if (partitioned) {
            conf.set(PARTITION_PATH_FIELD, "par");
        }

        List<Event> events = new ArrayList<>();
        events.addAll(generateEvents(tableId, partitioned));
        events.addAll(generateEvents(TableId.tableId("test_db", "test_table_2"), partitioned));
        events.addAll(generateEvents(tableId, partitioned));

        DataStream<Event> stream = env.fromData(events, TypeInformation.of(Event.class));
        HudiSink hoodieSink = new HudiSink(conf, "", ZoneId.systemDefault());
        stream.sinkTo(hoodieSink).uid("hudi_sink").name("hudi_sink");
        env.execute("Values to Hudi Sink");

        Thread.sleep(5000);

        List<String> actual = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        String[] expected =
                partitioned
                        ? new String[] {
                            "17,par1,17,updated,value,false,[1, 2, 4],[4, 5, 6, 8],246.80,15,150,111111111111,6.28,12.56,84,18629,7322000,1609545600000,1623456790,par1",
                            "21,par2,21,test,data,true,[13],[14, 15],999.99,30,300,777777777777,1.41,2.82,126,18630,10983000,1609632000000,1623456789,par2"
                        }
                        : new String[] {
                            "17,,17,updated,value,false,[1, 2, 4],[4, 5, 6, 8],246.80,15,150,111111111111,6.28,12.56,84,18629,7322000,1609545600000,1623456790,par1",
                            "21,,21,test,data,true,[13],[14, 15],999.99,30,300,777777777777,1.41,2.82,126,18630,10983000,1609632000000,1623456789,par2"
                        };
        while (actual.size() != expected.length) {
            if (System.currentTimeMillis() - startTime > DEFAULT_TIMEOUT_MS) {
                throw new RuntimeException("Timeout waiting for table to be populated");
            }
            actual =
                    fetchTableContent(
                            conf,
                            String.format(
                                    warehouse + "/%s/%s",
                                    tableId.getSchemaName(),
                                    tableId.getTableName()));
            Thread.sleep(1000);
        }
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    private List<Event> generateEvents(TableId tableId, boolean partitioned) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("f_char", DataTypes.CHAR(5), null))
                        .column(new PhysicalColumn("f_varchar", DataTypes.VARCHAR(20), null))
                        .column(new PhysicalColumn("f_boolean", DataTypes.BOOLEAN(), null))
                        .column(new PhysicalColumn("f_binary", DataTypes.BINARY(10), null))
                        .column(new PhysicalColumn("f_varbinary", DataTypes.VARBINARY(20), null))
                        .column(new PhysicalColumn("f_decimal", DataTypes.DECIMAL(10, 2), null))
                        .column(new PhysicalColumn("f_tinyint", DataTypes.TINYINT(), null))
                        .column(new PhysicalColumn("f_smallint", DataTypes.SMALLINT(), null))
                        .column(new PhysicalColumn("f_bigint", DataTypes.BIGINT(), null))
                        .column(new PhysicalColumn("f_float", DataTypes.FLOAT(), null))
                        .column(new PhysicalColumn("f_double", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("f_int", DataTypes.INT(), null))
                        .column(new PhysicalColumn("f_date", DataTypes.DATE(), null))
                        .column(new PhysicalColumn("f_time", DataTypes.TIME(), null))
                        .column(new PhysicalColumn("f_timestamp", DataTypes.TIMESTAMP(3), null))
                        .column(new PhysicalColumn("ts", DataTypes.BIGINT(), null))
                        .column(new PhysicalColumn("par", DataTypes.STRING(), null))
                        .primaryKey("id");
        if (partitioned) {
            schemaBuilder.partitionKey("par");
        }
        Schema schema = schemaBuilder.build();
        try (HudiMetadataApplier hudiMetadataApplier = new HudiMetadataApplier(catalogConf)) {
            hudiMetadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        }
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.CHAR(5),
                                DataTypes.VARCHAR(20),
                                DataTypes.BOOLEAN(),
                                DataTypes.BINARY(10),
                                DataTypes.VARBINARY(20),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.FLOAT(),
                                DataTypes.DOUBLE(),
                                DataTypes.INT(),
                                DataTypes.DATE(),
                                DataTypes.TIME(),
                                DataTypes.TIMESTAMP(3),
                                DataTypes.BIGINT(),
                                DataTypes.STRING()));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    BinaryStringData.fromString("hello"),
                                    BinaryStringData.fromString("world"),
                                    true,
                                    new byte[] {1, 2, 3},
                                    new byte[] {4, 5, 6, 7},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(123.45), 10, 2),
                                    (byte) 10,
                                    (short) 100,
                                    999999999999L,
                                    3.14f,
                                    6.28,
                                    42,
                                    18628, // days since epoch for 2021-01-01
                                    3661000, // milliseconds for 01:01:01.000
                                    TimestampData.fromMillis(1609459200000L), // 2021-01-01 00:00:00
                                    1623456789L,
                                    BinaryStringData.fromString("par1")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    BinaryStringData.fromString("foo"),
                                    BinaryStringData.fromString("bar"),
                                    false,
                                    new byte[] {8, 9},
                                    new byte[] {10, 11, 12},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(678.90), 10, 2),
                                    (byte) 20,
                                    (short) 200,
                                    888888888888L,
                                    2.71f,
                                    5.42,
                                    84,
                                    18629, // days since epoch for 2021-01-02
                                    7322000, // milliseconds for 02:02:02.000
                                    TimestampData.fromMillis(1609545600000L), // 2021-01-02 00:00:00
                                    1623456788L,
                                    BinaryStringData.fromString("par1")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    21,
                                    BinaryStringData.fromString("test"),
                                    BinaryStringData.fromString("data"),
                                    true,
                                    new byte[] {13},
                                    new byte[] {14, 15},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(999.99), 10, 2),
                                    (byte) 30,
                                    (short) 300,
                                    777777777777L,
                                    1.41f,
                                    2.82,
                                    126,
                                    18630, // days since epoch for 2021-01-03
                                    10983000, // milliseconds for 03:03:03.000
                                    TimestampData.fromMillis(1609632000000L), // 2021-01-03 00:00:00
                                    1623456789L,
                                    BinaryStringData.fromString("par2")
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    BinaryStringData.fromString("hello"),
                                    BinaryStringData.fromString("world"),
                                    true,
                                    new byte[] {1, 2, 3},
                                    new byte[] {4, 5, 6, 7},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(123.45), 10, 2),
                                    (byte) 10,
                                    (short) 100,
                                    999999999999L,
                                    3.14f,
                                    6.28,
                                    42,
                                    18628, // days since epoch for 2021-01-01
                                    3661000, // milliseconds for 01:01:01.000
                                    TimestampData.fromMillis(1609459200000L), // 2021-01-01 00:00:00
                                    1623456789L,
                                    BinaryStringData.fromString("par1")
                                }),
                        generator.generate(
                                new Object[] {
                                    17,
                                    BinaryStringData.fromString("updated"),
                                    BinaryStringData.fromString("value"),
                                    false,
                                    new byte[] {1, 2, 4},
                                    new byte[] {4, 5, 6, 8},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(246.80), 10, 2),
                                    (byte) 15,
                                    (short) 150,
                                    111111111111L,
                                    6.28f,
                                    12.56,
                                    84,
                                    18629, // days since epoch for 2021-01-02
                                    7322000, // milliseconds for 02:02:02.000
                                    TimestampData.fromMillis(1609545600000L), // 2021-01-02 00:00:00
                                    1623456790L,
                                    BinaryStringData.fromString("par1")
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    BinaryStringData.fromString("foo"),
                                    BinaryStringData.fromString("bar"),
                                    false,
                                    new byte[] {8, 9},
                                    new byte[] {10, 11, 12},
                                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                                            java.math.BigDecimal.valueOf(678.90), 10, 2),
                                    (byte) 20,
                                    (short) 200,
                                    888888888888L,
                                    2.71f,
                                    5.42,
                                    84,
                                    18629, // days since epoch for 2021-01-02
                                    7322000, // milliseconds for 02:02:02.000
                                    TimestampData.fromMillis(1609545600000L), // 2021-01-02 00:00:00
                                    1623456788L,
                                    BinaryStringData.fromString("par1")
                                })));
    }

    private List<String> fetchTableContent(Configuration conf, String basePath) throws Exception {
        conf.set(PATH, basePath);
        HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
        org.apache.avro.Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
        File hoodiePropertiesFile =
                new File(basePath + "/" + METAFOLDER_NAME + "/" + HOODIE_PROPERTIES_FILE);
        HoodieWriteConfig config =
                HoodieWriteConfig.newBuilder()
                        .fromFile(hoodiePropertiesFile)
                        .withMemoryConfig(
                                HoodieMemoryConfig.newBuilder()
                                        .withMaxMemoryMaxSize(
                                                FlinkOptions.WRITE_MERGE_MAX_MEMORY.defaultValue()
                                                        * 1024
                                                        * 1024L,
                                                FlinkOptions.COMPACTION_MAX_MEMORY.defaultValue()
                                                        * 1024
                                                        * 1024L)
                                        .build())
                        .withPath(basePath)
                        .build();
        HoodieFlinkTable<?> table =
                HoodieFlinkTable.create(config, HoodieFlinkEngineContext.DEFAULT, metaClient);

        String latestInstant =
                metaClient
                        .getActiveTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::requestedTime)
                        .orElse(null);

        List<FileSlice> fileSlices =
                table
                        .getSliceView()
                        .getAllLatestFileSlicesBeforeOrOn(latestInstant)
                        .values()
                        .stream()
                        .flatMap(s -> s)
                        .collect(Collectors.toList());
        List<String> readBuffer = new ArrayList<>();
        for (FileSlice fileSlice : fileSlices) {
            try (ClosableIterator<RowData> rowIterator =
                    getRecordIterator(fileSlice, schema, metaClient, config)) {
                while (rowIterator.hasNext()) {
                    readBuffer.add(filterOutVariables(schema, rowIterator.next()));
                }
            }
        }
        return readBuffer;
    }

    private static ClosableIterator<RowData> getRecordIterator(
            FileSlice fileSlice,
            org.apache.avro.Schema tableSchema,
            HoodieTableMetaClient metaClient,
            HoodieWriteConfig writeConfig)
            throws IOException {
        HoodieFileGroupReader<RowData> fileGroupReader =
                FormatUtils.createFileGroupReader(
                        metaClient,
                        writeConfig,
                        InternalSchemaManager.DISABLED,
                        fileSlice,
                        tableSchema,
                        tableSchema,
                        fileSlice.getLatestInstantTime(),
                        FlinkOptions.REALTIME_PAYLOAD_COMBINE,
                        false,
                        Collections.emptyList(),
                        Option.empty());
        return fileGroupReader.getClosableIterator();
    }

    private static String filterOutVariables(org.apache.avro.Schema schema, RowData record) {
        RowDataAvroQueryContexts.RowDataQueryContext queryContext =
                RowDataAvroQueryContexts.fromAvroSchema(schema);
        List<String> fields = new ArrayList<>();
        fields.add(getFieldValue(queryContext, record, "_hoodie_record_key"));
        fields.add(getFieldValue(queryContext, record, "_hoodie_partition_path"));
        fields.add(getFieldValue(queryContext, record, "id"));
        fields.add(getFieldValue(queryContext, record, "f_char"));
        fields.add(getFieldValue(queryContext, record, "f_varchar"));
        fields.add(getFieldValue(queryContext, record, "f_boolean"));
        fields.add(getFieldValue(queryContext, record, "f_binary"));
        fields.add(getFieldValue(queryContext, record, "f_varbinary"));
        fields.add(getFieldValue(queryContext, record, "f_decimal"));
        fields.add(getFieldValue(queryContext, record, "f_tinyint"));
        fields.add(getFieldValue(queryContext, record, "f_smallint"));
        fields.add(getFieldValue(queryContext, record, "f_bigint"));
        fields.add(getFieldValue(queryContext, record, "f_float"));
        fields.add(getFieldValue(queryContext, record, "f_double"));
        fields.add(getFieldValue(queryContext, record, "f_int"));
        fields.add(getFieldValue(queryContext, record, "f_date"));
        fields.add(getFieldValue(queryContext, record, "f_time"));
        fields.add(getFieldValue(queryContext, record, "f_timestamp"));
        fields.add(getFieldValue(queryContext, record, "ts"));
        fields.add(getFieldValue(queryContext, record, "par"));
        return String.join(",", fields);
    }

    private static String getFieldValue(
            RowDataAvroQueryContexts.RowDataQueryContext queryContext,
            RowData rowData,
            String fieldName) {
        Object val = queryContext.getFieldQueryContext(fieldName).getValAsJava(rowData, true);
        if (val instanceof ByteBuffer) {
            byte[] bytes = ((ByteBuffer) val).array();
            return Arrays.toString(bytes);
        }
        return String.valueOf(val);
    }
}
