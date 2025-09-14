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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.OperatorUidGenerator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.connectors.doris.sink.utils.DorisContainer;
import org.apache.flink.cdc.connectors.doris.sink.utils.DorisSinkTestBase;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.BENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.FENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_BATCH_MODE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.USERNAME;

/** IT tests for {@link DorisMetadataApplier}. */
class DorisMetadataApplierITCase extends DorisSinkTestBase {
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final int DATABASE_OPERATION_TIMEOUT_SECONDS = 5;

    @BeforeAll
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @BeforeEach
    public void initializeDatabase() {
        createDatabase(DorisContainer.DORIS_DATABASE_NAME);

        // waiting for table to be created
        DORIS_CONTAINER.waitForLog(
                String.format(".*createDb dbName = %s,.*\\s", DorisContainer.DORIS_DATABASE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Database {} created.", DorisContainer.DORIS_DATABASE_NAME);
    }

    @AfterEach
    public void destroyDatabase() {
        dropDatabase(DorisContainer.DORIS_DATABASE_NAME);
        // waiting for database to be created
        DORIS_CONTAINER.waitForLog(
                String.format(
                        ".*finish drop database\\[%s\\].*\\s", DorisContainer.DORIS_DATABASE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Database {} destroyed.", DorisContainer.DORIS_DATABASE_NAME);
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

    private List<Event> generateAlterColumnTypeWithDefaultValueEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null, "2.71828"))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null, "Alice"))
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

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisDataTypes(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), "ID"))
                        // Doris sink doesn't support BINARY type yet.
                        // .column(new PhysicalColumn("binary", DataTypes.BINARY(17), "Binary"))
                        // .column(new PhysicalColumn("varbinary", DataTypes.VARBINARY(17), "Var
                        // Binary"))
                        .column(new PhysicalColumn("bytes", DataTypes.BYTES(), "Bytes"))
                        .column(new PhysicalColumn("boolean", DataTypes.BOOLEAN(), "Boolean"))
                        .column(new PhysicalColumn("int", DataTypes.INT(), "Int"))
                        .column(new PhysicalColumn("tinyint", DataTypes.TINYINT(), "Tiny Int"))
                        .column(new PhysicalColumn("smallint", DataTypes.SMALLINT(), "Small Int"))
                        .column(new PhysicalColumn("float", DataTypes.FLOAT(), "Float"))
                        .column(new PhysicalColumn("double", DataTypes.DOUBLE(), "Double"))
                        .column(new PhysicalColumn("char", DataTypes.CHAR(17), "Char"))
                        .column(new PhysicalColumn("varchar", DataTypes.VARCHAR(17), "Var Char"))
                        .column(new PhysicalColumn("string", DataTypes.STRING(), "String"))
                        .column(new PhysicalColumn("decimal", DataTypes.DECIMAL(17, 7), "Decimal"))
                        .column(new PhysicalColumn("date", DataTypes.DATE(), "Date"))
                        // Doris sink doesn't support TIME type ，thus convert TIME to STRING
                        .column(new PhysicalColumn("time", DataTypes.TIME(), "Time"))
                        .column(
                                new PhysicalColumn(
                                        "time_3", DataTypes.TIME(3), "Time With Precision"))
                        .column(
                                new PhysicalColumn(
                                        "time_6", DataTypes.TIME(6), "Time With Precision"))
                        .column(new PhysicalColumn("timestamp", DataTypes.TIMESTAMP(), "Timestamp"))
                        .column(
                                new PhysicalColumn(
                                        "timestamp_3",
                                        DataTypes.TIMESTAMP(3),
                                        "Timestamp With Precision"))
                        .column(
                                new PhysicalColumn(
                                        "timestamptz", DataTypes.TIMESTAMP_TZ(), "TimestampTZ"))
                        .column(
                                new PhysicalColumn(
                                        "timestamptz_3",
                                        DataTypes.TIMESTAMP_TZ(3),
                                        "TimestampTZ With Precision"))
                        .column(
                                new PhysicalColumn(
                                        "timestampltz", DataTypes.TIMESTAMP_LTZ(), "TimestampLTZ"))
                        .column(
                                new PhysicalColumn(
                                        "timestampltz_3",
                                        DataTypes.TIMESTAMP_LTZ(3),
                                        "TimestampLTZ With Precision"))
                        .column(
                                new PhysicalColumn(
                                        "arrayofint",
                                        DataTypes.ARRAY(DataTypes.INT()),
                                        "Array of Int"))
                        .column(
                                new PhysicalColumn(
                                        "arrayofstr",
                                        DataTypes.ARRAY(DataTypes.STRING()),
                                        "Array of String"))
                        .column(
                                new PhysicalColumn(
                                        "mapint2str",
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                        "Map Int to String"))
                        .primaryKey("id")
                        .build();

        runJobWithEvents(
                Collections.singletonList(new CreateTableEvent(tableId, schema)), batchMode);

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "bytes | TEXT | Yes | false | null",
                        "boolean | BOOLEAN | Yes | false | null",
                        "int | INT | Yes | false | null",
                        "tinyint | TINYINT | Yes | false | null",
                        "smallint | SMALLINT | Yes | false | null",
                        "float | FLOAT | Yes | false | null",
                        "double | DOUBLE | Yes | false | null",
                        "char | CHAR(51) | Yes | false | null",
                        "varchar | VARCHAR(51) | Yes | false | null",
                        "string | TEXT | Yes | false | null",
                        "decimal | DECIMAL(17, 7) | Yes | false | null",
                        "date | DATE | Yes | false | null",
                        "time | TEXT | Yes | false | null",
                        "time_3 | TEXT | Yes | false | null",
                        "time_6 | TEXT | Yes | false | null",
                        "timestamp | DATETIME(6) | Yes | false | null",
                        "timestamp_3 | DATETIME(3) | Yes | false | null",
                        "timestamptz | DATETIME(6) | Yes | false | null",
                        "timestamptz_3 | DATETIME(3) | Yes | false | null",
                        "timestampltz | DATETIME(6) | Yes | false | null",
                        "timestampltz_3 | DATETIME(3) | Yes | false | null",
                        "arrayofint | TEXT | Yes | false | null",
                        "arrayofstr | TEXT | Yes | false | null",
                        "mapint2str | TEXT | Yes | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisAddColumn(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        runJobWithEvents(generateAddColumnEvents(tableId), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "number | DOUBLE | Yes | false | null",
                        "name | VARCHAR(51) | Yes | false | null",
                        "extra_date | DATE | Yes | false | null",
                        "extra_bool | BOOLEAN | Yes | false | null",
                        "extra_decimal | DECIMAL(17, 0) | Yes | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisDropColumn(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        runJobWithEvents(generateDropColumnEvents(tableId), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null", "name | VARCHAR(51) | Yes | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisRenameColumn(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        runJobWithEvents(generateRenameColumnEvents(tableId), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "kazu | DOUBLE | Yes | false | null",
                        "namae | VARCHAR(51) | Yes | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisAlterColumnType(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        runJobWithEvents(generateAlterColumnTypeEvents(tableId), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "number | DOUBLE | Yes | false | null",
                        "name | VARCHAR(57) | Yes | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisAlterColumnTypeWithDefaultValue(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        runJobWithEvents(generateAlterColumnTypeWithDefaultValueEvents(tableId), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "number | DOUBLE | Yes | false | 2.71828",
                        "name | VARCHAR(57) | Yes | false | Alice");

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisNarrowingAlterColumnType(boolean batchMode) {
        Assertions.assertThatThrownBy(
                        () -> {
                            TableId tableId =
                                    TableId.tableId(
                                            DorisContainer.DORIS_DATABASE_NAME,
                                            DorisContainer.DORIS_TABLE_NAME);

                            runJobWithEvents(
                                    generateNarrowingAlterColumnTypeEvents(tableId), batchMode);
                        })
                .isExactlyInstanceOf(JobExecutionException.class);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisTruncateTable(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> preparationTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 1, 2.3, "Alice")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 2, 3.4, "Bob")));
        runJobWithEvents(preparationTestingEvents, batchMode);
        waitAndVerify(
                tableId,
                3,
                Arrays.asList("1 | 2.3 | Alice", "2 | 3.4 | Bob"),
                DATABASE_OPERATION_TIMEOUT_SECONDS * 1000L);

        List<Event> truncateTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        new TruncateTableEvent(tableId),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 3, 4.5, "Cecily")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 4, 5.6, "Derrida")));
        runJobWithEvents(truncateTestingEvents, batchMode);
        waitAndVerify(
                tableId,
                3,
                Arrays.asList("3 | 4.5 | Cecily", "4 | 5.6 | Derrida"),
                DATABASE_OPERATION_TIMEOUT_SECONDS * 1000L);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testDorisDropTable(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> preparationTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 1, 2.3, "Alice")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 2, 3.4, "Bob")));
        runJobWithEvents(preparationTestingEvents, batchMode);

        waitAndVerify(
                tableId,
                3,
                Arrays.asList("1 | 2.3 | Alice", "2 | 3.4 | Bob"),
                DATABASE_OPERATION_TIMEOUT_SECONDS * 1000L);

        runJobWithEvents(
                Arrays.asList(new CreateTableEvent(tableId, schema), new DropTableEvent(tableId)),
                batchMode);

        Assertions.assertThatThrownBy(() -> fetchTableContent(tableId, 3))
                .isExactlyInstanceOf(SQLSyntaxErrorException.class)
                .hasMessageContaining(
                        String.format(
                                "errCode = 2, detailMessage = Unknown table '%s'",
                                tableId.getTableName()));
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testMysqlDefaultTimestampValueConversionInCreateTable(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(50), null))
                        .column(
                                new PhysicalColumn(
                                        "created_time",
                                        DataTypes.TIMESTAMP(),
                                        null,
                                        DorisSchemaUtils.INVALID_OR_MISSING_DATATIME))
                        .column(
                                new PhysicalColumn(
                                        "updated_time",
                                        DataTypes.TIMESTAMP_LTZ(),
                                        null,
                                        DorisSchemaUtils.INVALID_OR_MISSING_DATATIME))
                        .primaryKey("id")
                        .build();

        runJobWithEvents(
                Collections.singletonList(new CreateTableEvent(tableId, schema)), batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(150) | Yes | false | null",
                        "created_time | DATETIME(6) | Yes | false | "
                                + DorisSchemaUtils.DEFAULT_DATETIME,
                        "updated_time | DATETIME(6) | Yes | false | "
                                + DorisSchemaUtils.DEFAULT_DATETIME);

        assertEqualsInOrder(expected, actual);
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testMysqlDefaultTimestampValueConversionInAddColumn(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        Schema initialSchema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(50), null))
                        .primaryKey("id")
                        .build();

        List<Event> events = new ArrayList<>();
        events.add(new CreateTableEvent(tableId, initialSchema));

        PhysicalColumn createdTimeCol =
                new PhysicalColumn(
                        "created_time",
                        DataTypes.TIMESTAMP(),
                        null,
                        DorisSchemaUtils.INVALID_OR_MISSING_DATATIME);

        PhysicalColumn updatedTimeCol =
                new PhysicalColumn(
                        "updated_time",
                        DataTypes.TIMESTAMP_LTZ(),
                        null,
                        DorisSchemaUtils.INVALID_OR_MISSING_DATATIME);

        events.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(createdTimeCol))));

        events.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(updatedTimeCol))));

        runJobWithEvents(events, batchMode);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(150) | Yes | false | null",
                        "created_time | DATETIME(6) | Yes | false | "
                                + DorisSchemaUtils.DEFAULT_DATETIME,
                        "updated_time | DATETIME(6) | Yes | false | "
                                + DorisSchemaUtils.DEFAULT_DATETIME);

        assertEqualsInOrder(expected, actual);
    }

    private void runJobWithEvents(List<Event> events, boolean batchMode) throws Exception {
        DataStream<Event> stream =
                env.fromCollection(events, TypeInformation.of(Event.class)).setParallelism(1);

        Configuration config =
                new Configuration()
                        .set(FENODES, DORIS_CONTAINER.getFeNodes())
                        .set(BENODES, DORIS_CONTAINER.getBeNodes())
                        .set(USERNAME, DorisContainer.DORIS_USERNAME)
                        .set(PASSWORD, DorisContainer.DORIS_PASSWORD)
                        .set(SINK_ENABLE_BATCH_MODE, batchMode)
                        .set(SINK_ENABLE_DELETE, true);

        config.addAll(
                Configuration.fromMap(
                        Collections.singletonMap("table.create.properties.replication_num", "1")));

        DataSink dorisSink = createDorisDataSink(config);

        String schemaOperatorUid = "$$_schema_operator_$$";

        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.EVOLVE,
                        schemaOperatorUid,
                        DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT,
                        "UTC");

        OperatorIDGenerator schemaOperatorIDGenerator = new OperatorIDGenerator(schemaOperatorUid);

        stream =
                schemaOperatorTranslator.translateRegular(
                        stream,
                        DEFAULT_PARALLELISM,
                        dorisSink
                                .getMetadataApplier()
                                .setAcceptedSchemaEvolutionTypes(
                                        Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                                                .collect(Collectors.toSet())),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                new SinkDef("doris", "Dummy Doris Sink", config),
                stream,
                dorisSink,
                schemaOperatorIDGenerator.generate(),
                new OperatorUidGenerator());

        env.execute("Doris Schema Evolution Test");
    }

    BinaryRecordData generate(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }

    private void waitAndVerify(
            TableId tableId, int numberOfColumns, List<String> expected, long timeoutMilliseconds)
            throws Exception {
        long timeout = System.currentTimeMillis() + timeoutMilliseconds;
        while (System.currentTimeMillis() < timeout) {
            List<String> actual = fetchTableContent(tableId, numberOfColumns);
            if (expected.stream()
                    .sorted()
                    .collect(Collectors.toList())
                    .equals(actual.stream().sorted().collect(Collectors.toList()))) {
                return;
            }
            LOG.info(
                    "Content of {} isn't ready.\nExpected: {}\nActual: {}",
                    tableId,
                    expected,
                    actual);
            Thread.sleep(1000L);
        }
        Assertions.fail("Failed to verify content of {}.", tableId);
    }
}
