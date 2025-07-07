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

package org.apache.flink.cdc.connectors.fluss;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_1;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_2;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for Fluss Pipeline. */
public class FlussPipelineITCase {
    private static final int MAX_PARALLELISM = 4;

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .build());

    /**
     * Use {@link FlussClusterExtension} to start a Fluss cluster with sasl authentication for every
     * test case.
     */
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setNumOfTabletServers(3)
                    .build();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    static final String CATALOG_NAME = "test_catalog";
    static final String DEFAULT_DB = "default_schema";

    protected TableEnvironment tBatchEnv;

    @BeforeEach
    void before() {
        // open a catalog so that we can get table from the catalog
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();

        // create batch table environment
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        // create database
        tBatchEnv.executeSql("create database " + DEFAULT_DB);
        tBatchEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testSinglePrimaryTable() throws Exception {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("a")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("b")
                                }));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("c")
                                }));
        split1.add(insertEvent3);
        eventOfSplits.add(split1);
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("b")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("b2")
                                }));
        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("a")
                                }));
        eventOfSplits.add(Collections.singletonList(updateEvent));
        eventOfSplits.add(Collections.singletonList(deleteEvent));

        composeAndExecute(eventOfSplits);
        checkResult(TABLE_1, Arrays.asList("+I[2, b2]", "+I[3, c]"));
    }

    @Test
    void testSingleLogTable() throws Exception {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("a")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("b")
                                }));
        split1.add(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("c")
                                }));
        split1.add(insertEvent3);
        eventOfSplits.add(split1);

        composeAndExecute(eventOfSplits);
        checkResult(TABLE_1, Arrays.asList("+I[1, a]", "+I[2, b]", "+I[3, c]"));
    }

    @Test
    void testSingleLogTableWithSchemaChange() {
        assertThatThrownBy(() -> composeAndExecute(ValuesDataSourceHelper.singleSplitSingleTable()))
                .rootCause()
                .hasMessageContaining(
                        "fluss metadata applier only support CreateTableEvent now but receives AddColumnEvent");
    }

    @Test
    void testLackUsernameAndPassword() {
        assertThatThrownBy(
                        () ->
                                composeAndExecute(
                                        ValuesDataSourceHelper.singleSplitSingleTable(),
                                        Collections.singletonMap(
                                                BOOTSTRAP_SERVERS.key(), getBootstrapServers())))
                .rootCause()
                .hasMessageContaining(
                        "The connection has not completed authentication yet. This may be caused by a missing or incorrect configuration of 'client.security.protocol' on the client side.");
    }

    @Test
    void testWrongTableOptions() {
        Map<String, String> sinkOption = new HashMap<>();
        sinkOption.put(BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        sinkOption.put(BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        sinkOption.put("properties.client.security.protocol", "sasl");
        sinkOption.put("properties.client.security.sasl.mechanism", "PLAIN");
        sinkOption.put("properties.client.security.sasl.username", "guest");
        sinkOption.put("properties.client.security.sasl.password", "password2");
        sinkOption.put("properties.table.non-key", "non-key-value");
        assertThatThrownBy(
                        () ->
                                composeAndExecute(
                                        ValuesDataSourceHelper.singleSplitSingleTable(),
                                        sinkOption))
                .rootCause()
                .hasMessageContaining("'table.non-key' is not a Fluss table property");
    }

    @Test
    void testMultiTables() throws Exception {
        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        DataChangeEvent insertTable1Event1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertTable1Event1);
        CreateTableEvent createTableEvent2 = new CreateTableEvent(TABLE_2, schema);
        split1.add(createTableEvent2);
        DataChangeEvent insertTable1Event2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertTable1Event2);
        DataChangeEvent insertTable2Event1 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertTable2Event1);
        DataChangeEvent insertTable1Event3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split1.add(insertTable1Event3);

        DataChangeEvent insertTable2Event2 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertTable2Event2);
        DataChangeEvent insertTabl2Event3 =
                DataChangeEvent.insertEvent(
                        TABLE_2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        split1.add(insertTabl2Event3);
        eventOfSplits.add(split1);

        composeAndExecute(eventOfSplits);
        checkResult(TABLE_1, Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[3, 3]"));
        checkResult(TABLE_2, Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[3, 3]"));
    }

    @Test
    void testInsertExistTableWithMoreColumns() throws Exception {
        // create a fluss table with 2 columns but then cdc read a source table with 3 columns
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    col2 STRING,\n"
                                        + "    col1 STRING,\n"
                                        + "   PRIMARY KEY (col1) NOT ENFORCED \n"
                                        + ");",
                                TABLE_1.getSchemaName(), TABLE_1.getTableName()))
                .await();

        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("a"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        eventOfSplits.add(split1);

        composeAndExecute(eventOfSplits);
        checkResult(TABLE_1, Arrays.asList("+I[a, 1]", "+I[2, 2]"));
    }

    @Test
    void testInsertExistTableWithLessColumns() throws Exception {
        // create a fluss table with 2 columns but then cdc read a source table with 3 columns
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    col1 STRING,\n"
                                        + "    col2 STRING,\n"
                                        + "    col3 STRING,\n"
                                        + "   PRIMARY KEY (col1) NOT ENFORCED \n"
                                        + ");",
                                TABLE_1.getSchemaName(), TABLE_1.getTableName()))
                .await();

        List<List<Event>> eventOfSplits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        split1.add(createTableEvent);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("a")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        eventOfSplits.add(split1);

        composeAndExecute(eventOfSplits);
        checkResult(TABLE_1, Arrays.asList("+I[1, a, null]", "+I[2, 2, null]"));
    }

    private void composeAndExecute(List<List<Event>> customSourceEvents) throws Exception {
        Map<String, String> sinkOption = new HashMap<>();
        sinkOption.put(BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        sinkOption.put("properties.client.security.protocol", "sasl");
        sinkOption.put("properties.client.security.sasl.mechanism", "PLAIN");
        sinkOption.put("properties.client.security.sasl.username", "guest");
        sinkOption.put("properties.client.security.sasl.password", "password2");
        composeAndExecute(customSourceEvents, sinkOption);
    }

    private void composeAndExecute(
            List<List<Event>> customSourceEvents, Map<String, String> sinkOption) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);
        ValuesDataSourceHelper.setSourceEvents(customSourceEvents);

        // Setup value sink
        SinkDef sinkDef = new SinkDef("fluss", "Fluss Sink", Configuration.fromMap(sinkOption));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 4);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
    }

    private void checkResult(TableId tableId, List<String> expectedRows) {
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s.%s limit %d",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        expectedRows.size()))
                        .collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    private static com.alibaba.fluss.config.Configuration initConfig() {
        com.alibaba.fluss.config.Configuration conf = new com.alibaba.fluss.config.Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // set security information.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        return conf;
    }

    String getBootstrapServers() {
        return String.join(
                ",",
                FLUSS_CLUSTER_EXTENSION
                        .getClientConfig("CLIENT")
                        .get(ConfigOptions.BOOTSTRAP_SERVERS));
    }
}
