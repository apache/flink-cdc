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

package org.apache.flink.cdc.connectors.mysql.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSinkFactory;
import org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/** IT cases for MySQL Data Sink. */
public class MySqlDataSinkITCase extends MySqlSinkTestBase {

    private static final TableId TABLE_ID =
            TableId.tableId(MYSQL_CONTAINER.getDatabaseName(), "data_sink_test_table");

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Before
    public void initializeDatabase() {
        executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;", MYSQL_CONTAINER.getDatabaseName()));
        LOG.info("Database {} created.", MYSQL_CONTAINER.getDatabaseName());
    }

    @After
    public void destroyDatabase() {
        executeSql(String.format("DROP DATABASE %s;", MYSQL_CONTAINER.getDatabaseName()));
        LOG.info("Database {} destroyed.", MYSQL_CONTAINER.getDatabaseName());
    }

    @Test
    public void testSyncInsertEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            23, 1.41421356, BinaryStringData.fromString("Cicada")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            29, 9.973627192, BinaryStringData.fromString("Derrida")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly(
                        "17 | 3.141592653 | Alice",
                        "19 | 2.718281828 | Bob",
                        "23 | 1.41421356 | Cicada",
                        "29 | 9.973627192 | Derrida");
    }

    @Test
    public void testSyncUpdateEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        }),
                                generator.generate(
                                        new Object[] {
                                            17, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 2.718281828 | Bob");
    }

    @Test
    public void testSyncDeleteEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.deleteEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 3.141592653 | Alice");
    }

    private void runJobWithEvents(List<Event> events) throws Exception {
        DataStream<Event> stream = env.fromCollection(events, TypeInformation.of(Event.class));

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(MySqlDataSourceOptions.HOSTNAME, MYSQL_CONTAINER.getHost());
        sinkConfig.set(MySqlDataSourceOptions.PORT, MYSQL_CONTAINER.getDatabasePort());
        sinkConfig.set(MySqlDataSourceOptions.USERNAME, MYSQL_CONTAINER.getUsername());
        sinkConfig.set(MySqlDataSourceOptions.PASSWORD, MYSQL_CONTAINER.getPassword());
        sinkConfig.set(MySqlDataSinkOptions.SERVER_TIME_ZONE, "UTC");

        SinkDef sinkDef = new SinkDef(MySqlDataSinkFactory.IDENTIFIER, "MySql Sink", sinkConfig);

        DataSink dataSink = createMySqlDataSink(sinkConfig);

        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.EVOLVE,
                        "$$_schema_operator_$$",
                        DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT,
                        "UTC");

        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        stream =
                schemaOperatorTranslator.translateRegular(
                        stream,
                        DEFAULT_PARALLELISM,
                        dataSink.getMetadataApplier(),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(sinkDef, stream, dataSink, schemaOperatorIDGenerator.generate());

        env.execute("MySql Schema Evolution Test");
    }
}
