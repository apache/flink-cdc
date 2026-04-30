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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
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
import org.apache.flink.cdc.connectors.dws.utils.DwsContainer;
import org.apache.flink.cdc.connectors.dws.utils.DwsSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.URL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.USERNAME;

/** Integration tests for {@link DwsMetadataApplier}. */
class DwsMetadataApplierITCase extends DwsSinkTestBase {
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @BeforeEach
    public void initializeDatabase() {
        createDatabase(DwsContainer.DWS_DATABASE_TEST);
        createSchema(DwsContainer.DWS_DATABASE_TEST, DwsContainer.DWS_SCHEMA);
        dropTable(
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME));
        LOG.info("Database {} created.", DwsContainer.DWS_DATABASE_TEST);
    }

    @AfterEach
    public void destroyDatabase() {
        dropDatabase(DwsContainer.DWS_DATABASE_TEST);
        LOG.info("Database {} destroyed.", DwsContainer.DWS_DATABASE_TEST);
    }

    private List<Event> generateAddColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DECIMAL(10, 4), null))
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

    @Test
    void testDwsCreateTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DECIMAL(10, 4), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        runJobWithEvents(Collections.singletonList(new CreateTableEvent(tableId, schema)));

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | integer | NO | null",
                        "number | numeric | YES | null",
                        "name | character varying | YES | null");
        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testDwsAddColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        runJobWithEvents(generateAddColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | integer | NO | null",
                        "number | numeric | YES | null",
                        "name | character varying | YES | null",
                        "extra_date | date | YES | null",
                        "extra_bool | boolean | YES | null",
                        "extra_decimal | numeric | YES | null");
        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testDwsDropColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        runJobWithEvents(generateDropColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList("id | integer | NO | null", "name | character varying | YES | null");
        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testDwsRenameColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        runJobWithEvents(generateRenameColumnEvents(tableId));

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | integer | NO | null",
                        "kazu | double precision | YES | null",
                        "namae | character varying | YES | null");
        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testDwsAlterColumnType() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);
        runJobWithEvents(generateAlterColumnTypeEvents(tableId));
        Assertions.assertThat(getCharacterMaximumLength(tableId, "name")).isEqualTo(19);
    }

    @Test
    void testDwsAlterColumnTypeWithDefaultValue() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        runJobWithEvents(generateAlterColumnTypeWithDefaultValueEvents(tableId));
        Assertions.assertThat(getCharacterMaximumLength(tableId, "name")).isEqualTo(19);
    }

    @Test
    void testDwsNarrowingAlterColumnType() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);
        runJobWithEvents(generateNarrowingAlterColumnTypeEvents(tableId));
        Assertions.assertThat(getDataType(tableId, "number")).isEqualTo("real");
    }

    @Test
    void testDwsTruncateTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> truncateTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema), new TruncateTableEvent(tableId));

        runJobWithEvents(truncateTestingEvents);

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | integer | NO | null",
                        "number | double precision | YES | null",
                        "name | character varying | YES | null");
        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testDwsDropTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE_TEST,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> dropTestingEvents =
                Arrays.asList(new CreateTableEvent(tableId, schema), new DropTableEvent(tableId));

        runJobWithEvents(dropTestingEvents);
        Assertions.assertThat(tableExists(tableId)).isFalse();
    }

    private void runJobWithEvents(List<Event> events) throws Exception {
        DataStream<Event> stream =
                env.fromCollection(events, TypeInformation.of(Event.class)).setParallelism(1);

        Configuration config =
                new Configuration()
                        .set(URL, DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST))
                        .set(USERNAME, DwsContainer.DWS_USERNAME)
                        .set(PASSWORD, DwsContainer.DWS_PASSWORD)
                        .set(SINK_ENABLE_DELETE, true);

        DataSink dwsSink = createDwsDataSink(config);

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
                        dwsSink.getMetadataApplier()
                                .setAcceptedSchemaEvolutionTypes(
                                        Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                                                .collect(Collectors.toSet())),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                new SinkDef("dws", "Dummy dws sink", config),
                stream,
                dwsSink,
                schemaOperatorIDGenerator.generate(),
                new OperatorUidGenerator());

        env.execute("dws Schema Evolution Test");
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

    private Integer getCharacterMaximumLength(TableId tableId, String columnName)
            throws SQLException {
        try (java.sql.Connection conn = createConnection(tableId);
                java.sql.Statement statement = conn.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT character_maximum_length FROM information_schema.columns "
                                                + "WHERE table_schema='%s' AND table_name='%s' AND column_name='%s'",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        columnName))) {
            if (!rs.next()) {
                throw new AssertionError("Column not found: " + columnName);
            }
            return (Integer) rs.getObject(1);
        }
    }

    private String getDataType(TableId tableId, String columnName) throws SQLException {
        try (java.sql.Connection conn = createConnection(tableId);
                java.sql.Statement statement = conn.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT data_type FROM information_schema.columns "
                                                + "WHERE table_schema='%s' AND table_name='%s' AND column_name='%s'",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        columnName))) {
            if (!rs.next()) {
                throw new AssertionError("Column not found: " + columnName);
            }
            return rs.getString(1);
        }
    }
}
