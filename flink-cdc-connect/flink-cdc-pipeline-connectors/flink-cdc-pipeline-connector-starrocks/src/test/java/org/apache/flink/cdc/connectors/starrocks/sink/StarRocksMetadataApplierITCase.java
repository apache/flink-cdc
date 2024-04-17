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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksSinkTestBase;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.LOAD_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.USERNAME;

/** IT tests for {@link StarRocksMetadataApplier}. */
public class StarRocksMetadataApplierITCase extends StarRocksSinkTestBase {
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
        createDatabase(StarRocksContainer.STARROCKS_DATABASE_NAME);
        LOG.info("Database {} created.", StarRocksContainer.STARROCKS_DATABASE_NAME);
    }

    @After
    public void destroyDatabase() {
        dropDatabase(StarRocksContainer.STARROCKS_DATABASE_NAME);
        LOG.info("Database {} destroyed.", StarRocksContainer.STARROCKS_DATABASE_NAME);
    }

    private List<Event> generateAddColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn("extra_date", DataTypes.DATE(), null)))),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_bool", DataTypes.BOOLEAN(), null)))),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_decimal",
                                                DataTypes.DECIMAL(17, 0),
                                                null)))));
    }

    private List<Event> generateDropColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new DropColumnEvent(tableId, Collections.singletonList("number")));
    }

    private List<Event> generateRenameColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new RenameColumnEvent(tableId, Collections.singletonMap("number", "kazu")),
                new RenameColumnEvent(tableId, Collections.singletonMap("name", "namae")));
    }

    private List<Event> generateAlterColumnTypeEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("name", DataTypes.VARCHAR(19))));
    }

    private List<Event> generateNarrowingAlterColumnTypeEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                // Double -> Float is a narrowing cast, should fail
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("number", DataTypes.FLOAT())));
    }

    @Test
    public void testStarRocksAddColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        runJobWithEvents(generateAddColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "number | double | YES | false | null",
                        "name | varchar(51) | YES | false | null",
                        "extra_date | date | YES | false | null",
                        "extra_bool | boolean | YES | false | null",
                        "extra_decimal | decimal(17,0) | YES | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @Test
    public void testStarRocksDropColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        runJobWithEvents(generateDropColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null", "name | varchar(51) | YES | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @Test
    @Ignore("Rename column is not supported currently.")
    public void testStarRocksRenameColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        runJobWithEvents(generateRenameColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "kazu | double | YES | false | null",
                        "namae | varchar(51) | YES | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @Test
    @Ignore("Alter column is not supported currently.")
    public void testStarRocksAlterColumnType() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        runJobWithEvents(generateAlterColumnTypeEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "number | double | YES | false | null",
                        "name | varchar(57) | YES | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @Test(expected = JobExecutionException.class)
    public void testStarRocksNarrowingAlterColumnType() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        runJobWithEvents(generateNarrowingAlterColumnTypeEvents(tableId));
    }

    private void runJobWithEvents(List<Event> events) throws Exception {
        DataStream<Event> stream = env.fromCollection(events, TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD);

        DataSink starRocksSink = createStarRocksDataSink(config);

        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(SchemaChangeBehavior.EVOLVE, "$$_schema_operator_$$");

        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        stream =
                schemaOperatorTranslator.translate(
                        stream,
                        DEFAULT_PARALLELISM,
                        starRocksSink.getMetadataApplier(),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                new SinkDef("starrocks", "Dummy StarRocks Sink", config),
                stream,
                starRocksSink,
                schemaOperatorIDGenerator.generate());

        env.execute("StarRocks Schema Evolution Test");
    }
}
