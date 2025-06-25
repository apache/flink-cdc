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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.fluss.factory.FlussDataSinkFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BOOTSTRAP_SERVERS;

/** IT tests for {@link FlussDataSink}. */
class FlussPipelineITCase extends FlinkTestBase {
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private static final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
    private static final String CATALOG_NAME = "fluss_catalog";
    private static final String DEFAULT_SINK_DB = "flink_cdc_test_db";

    private static final TablePath DEFAULT_SINK_TABLE_1_PATH =
            TablePath.of(DEFAULT_SINK_DB, "flink_cdc_test_table_1");
    private static final TablePath DEFAULT_SINK_TABLE_2_PATH =
            TablePath.of(DEFAULT_SINK_DB, "flink_cdc_test_table_2");

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", com.alibaba.fluss.types.DataTypes.INT())
                                    .column("name", com.alibaba.fluss.types.DataTypes.CHAR(10))
                                    .build())
                    .build();

    @Test
    public void testCreateDatabase() throws Exception {
        admin.createDatabase(
                DEFAULT_SINK_TABLE_1_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        Assertions.assertThat(DEFAULT_SINK_DB)
                .isEqualTo(admin.getDatabaseInfo(DEFAULT_SINK_DB).get().getDatabaseName());
    }

    private List<Event> generateEvents(TableId tableId) {
        org.apache.flink.cdc.common.schema.Schema schema =
                org.apache.flink.cdc.common.schema.Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .column(new PhysicalColumn("birthday", DataTypes.TIMESTAMP_LTZ(6), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.DOUBLE(),
                                DataTypes.VARCHAR(17),
                                DataTypes.TIMESTAMP_LTZ(6)));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    3.14,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    2.718,
                                    BinaryStringData.fromString("Que Sera Sera"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    21,
                                    1.732,
                                    BinaryStringData.fromString("Disenchanted"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    2.718,
                                    BinaryStringData.fromString("Que Sera Sera"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    3.14,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                }),
                        generator.generate(
                                new Object[] {
                                    17,
                                    6.28,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-02T00:00:00.000Z"))
                                })));
    }

    public static DataSink createFlussDataSink(Configuration factoryConfiguration) {
        FlussDataSinkFactory factory = new FlussDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    static class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        }

        @Override
        public ClassLoader getClassLoader() {
            return null;
        }
    }

    @BeforeEach
    void before() throws Exception {
        env.setParallelism(1);
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        admin.createDatabase(
                DEFAULT_SINK_TABLE_1_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", com.alibaba.fluss.types.DataTypes.INT())
                                        .column(
                                                "number",
                                                com.alibaba.fluss.types.DataTypes.DOUBLE())
                                        .column("name", com.alibaba.fluss.types.DataTypes.STRING())
                                        .column(
                                                "birthday",
                                                com.alibaba.fluss.types.DataTypes.TIMESTAMP_LTZ(6))
                                        .build())
                        .distributedBy(1, "id")
                        .build();
        createTable(DEFAULT_SINK_TABLE_1_PATH, tableDescriptor);
        createTable(DEFAULT_SINK_TABLE_2_PATH, tableDescriptor);
    }

    @Test
    void testValuesToFlussMultiTable() throws Exception {
        TableId tableId1 =
                TableId.tableId(DEFAULT_SINK_DB, DEFAULT_SINK_TABLE_1_PATH.getTableName());
        TableId tableId2 =
                TableId.tableId(DEFAULT_SINK_DB, DEFAULT_SINK_TABLE_2_PATH.getTableName());
        List<Event> events = new ArrayList<>();
        events.addAll(generateEvents(tableId2));
        events.addAll(generateEvents(tableId1));
        DataStream<Event> stream = env.fromCollection(events, TypeInformation.of(Event.class));

        HashMap<String, String> conf = new HashMap<>();
        conf.put(BOOTSTRAP_SERVERS.key(), clientConf.toMap().get(BOOTSTRAP_SERVERS.key()));
        Configuration config = Configuration.fromMap(conf);

        Sink<Event> flussSink =
                ((FlinkSinkProvider) createFlussDataSink(config).getEventSinkProvider()).getSink();
        stream.sinkTo(flussSink);

        env.execute("Values to StarRocks Sink");

        CloseableIterator<Row> iterRows1 =
                tEnv.executeSql(
                                String.format(
                                        "SELECT * FROM %s LIMIT 10", DEFAULT_SINK_TABLE_1_PATH))
                        .collect();
        List<String> actual1 = assertAndCollectRecords(iterRows1, 2);
        List<String> expected =
                Arrays.asList(
                        "+I[17, 6.28, StarRocks, 2023-01-02T00:00:00Z]",
                        "+I[21, 1.732, Disenchanted, 2023-01-01T00:00:00Z]");
        Assertions.assertThat(actual1).containsExactlyInAnyOrderElementsOf(expected);

        CloseableIterator<Row> iterRows2 =
                tEnv.executeSql(
                                String.format(
                                        "SELECT * FROM %s LIMIT 10", DEFAULT_SINK_TABLE_2_PATH))
                        .collect();
        List<String> actual2 = assertAndCollectRecords(iterRows2, 2);
        Assertions.assertThat(actual2).containsExactlyInAnyOrderElementsOf(expected);
    }
}
