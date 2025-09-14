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
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.LOAD_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.USERNAME;

/** IT tests for {@link StarRocksMetadataApplier}. */
class StarRocksMetadataApplierITCase extends StarRocksSinkTestBase {
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
        executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
        LOG.info("Database {} created.", StarRocksContainer.STARROCKS_DATABASE_NAME);
    }

    @AfterEach
    public void destroyDatabase() {
        executeSql(String.format("DROP DATABASE %s;", StarRocksContainer.STARROCKS_DATABASE_NAME));
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
    void testStarRocksDataType() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), "ID"))
                        // StarRocks sink doesn't support BINARY and BYTES type yet.
                        // .column(new PhysicalColumn("binary", DataTypes.BINARY(17), "Binary"))
                        // .column(new PhysicalColumn("varbinary", DataTypes.VARBINARY(17), "Var
                        // Binary"))
                        // .column(new PhysicalColumn("bytes", DataTypes.BYTES(), "Bytes"))
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
                        // StarRocks sink doesn't support TIME type yet.
                        // .column(new PhysicalColumn("time", DataTypes.TIME(), "Time"))
                        // .column(new PhysicalColumn("time_3", DataTypes.TIME(3), "Time With
                        // Precision"))
                        .column(new PhysicalColumn("timestamp", DataTypes.TIMESTAMP(), "Timestamp"))
                        .column(
                                new PhysicalColumn(
                                        "timestamp_3",
                                        DataTypes.TIMESTAMP(3),
                                        "Timestamp With Precision"))
                        // StarRocks sink doesn't support TIMESTAMP with non-local TZ yet.
                        // .column(new PhysicalColumn("timestamptz", DataTypes.TIMESTAMP_TZ(),
                        // "TimestampTZ"))
                        // .column(new PhysicalColumn("timestamptz_3", DataTypes.TIMESTAMP_TZ(3),
                        // "TimestampTZ With Precision"))
                        .column(
                                new PhysicalColumn(
                                        "timestampltz", DataTypes.TIMESTAMP_LTZ(), "TimestampLTZ"))
                        .column(
                                new PhysicalColumn(
                                        "timestampltz_3",
                                        DataTypes.TIMESTAMP_LTZ(3),
                                        "TimestampLTZ With Precision"))
                        .primaryKey("id")
                        .build();

        runJobWithEvents(Collections.singletonList(new CreateTableEvent(tableId, schema)));

        List<String> actual = inspectTableSchema(tableId);
        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "boolean | boolean | YES | false | null",
                        "int | int | YES | false | null",
                        "tinyint | tinyint | YES | false | null",
                        "smallint | smallint | YES | false | null",
                        "float | float | YES | false | null",
                        "double | double | YES | false | null",
                        "char | char(51) | YES | false | null",
                        "varchar | varchar(51) | YES | false | null",
                        "string | varchar(1048576) | YES | false | null",
                        "decimal | decimal(17,7) | YES | false | null",
                        "date | date | YES | false | null",
                        "timestamp | datetime | YES | false | null",
                        "timestamp_3 | datetime | YES | false | null",
                        "timestampltz | datetime | YES | false | null",
                        "timestampltz_3 | datetime | YES | false | null");

        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testStarRocksAddColumn() throws Exception {
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
    void testStarRocksDropColumn() throws Exception {
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
    @Disabled("Rename column is not supported currently.")
    void testStarRocksRenameColumn() throws Exception {
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
    @Disabled("Alter column type is not supported currently.")
    void testStarRocksAlterColumnType() throws Exception {
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

    @Test
    @Disabled("Alter column type is not supported currently.")
    void testStarRocksNarrowingAlterColumnType() throws Exception {
        Assertions.assertThatThrownBy(
                () -> {
                    TableId tableId =
                            TableId.tableId(
                                    StarRocksContainer.STARROCKS_DATABASE_NAME,
                                    StarRocksContainer.STARROCKS_TABLE_NAME);

                    runJobWithEvents(generateNarrowingAlterColumnTypeEvents(tableId));
                });
    }

    @Test
    public void testStarRocksTruncateTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> truncateTableTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 1, 2.3, "Alice")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 2, 3.4, "Bob")),
                        new TruncateTableEvent(tableId),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 3, 4.5, "Cecily")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 4, 5.6, "Derrida")));
        runJobWithEvents(truncateTableTestingEvents);

        assertEqualsInAnyOrder(
                Arrays.asList("3 | 4.5 | Cecily", "4 | 5.6 | Derrida"),
                fetchTableContent(tableId, 3));
    }

    @Test
    public void testStarRocksDropTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        List<Event> dropTableTestingEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 1, 2.3, "Alice")),
                        DataChangeEvent.insertEvent(tableId, generate(schema, 2, 3.4, "Bob")),
                        new DropTableEvent(tableId));
        runJobWithEvents(dropTableTestingEvents);

        Assertions.assertThatThrownBy(() -> fetchTableContent(tableId, 3))
                .isExactlyInstanceOf(MySQLSyntaxErrorException.class)
                .hasMessageContaining(
                        String.format(
                                "Getting analyzing error. Detail message: Unknown table '%s.%s'.",
                                tableId.getSchemaName(), tableId.getTableName()));
    }

    private void runJobWithEvents(List<Event> events) throws Exception {
        DataStream<Event> stream =
                env.fromCollection(events, TypeInformation.of(Event.class)).setParallelism(1);

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD);

        DataSink starRocksSink = createStarRocksDataSink(config);

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
                        starRocksSink
                                .getMetadataApplier()
                                .setAcceptedSchemaEvolutionTypes(
                                        Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                                                .collect(Collectors.toSet())),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                new SinkDef("starrocks", "Dummy StarRocks Sink", config),
                stream,
                starRocksSink,
                schemaOperatorIDGenerator.generate(),
                new OperatorUidGenerator());

        env.execute("StarRocks Schema Evolution Test");
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

    @Test
    void testMysqlDefaultTimestampValueConversionInCreateTable() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(50), null))
                        .column(
                                new PhysicalColumn(
                                        "created_time",
                                        DataTypes.TIMESTAMP(),
                                        null,
                                        StarRocksUtils.INVALID_OR_MISSING_DATATIME))
                        .column(
                                new PhysicalColumn(
                                        "updated_time",
                                        DataTypes.TIMESTAMP_LTZ(),
                                        null,
                                        StarRocksUtils.INVALID_OR_MISSING_DATATIME))
                        .primaryKey("id")
                        .build();

        runJobWithEvents(Collections.singletonList(new CreateTableEvent(tableId, schema)));

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(150) | YES | false | null",
                        "created_time | datetime | YES | false | "
                                + StarRocksUtils.DEFAULT_DATETIME,
                        "updated_time | datetime | YES | false | "
                                + StarRocksUtils.DEFAULT_DATETIME);

        assertEqualsInOrder(expected, actual);
    }

    @Test
    void testMysqlDefaultTimestampValueConversionInAddColumn() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);

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
                        StarRocksUtils.INVALID_OR_MISSING_DATATIME);

        PhysicalColumn updatedTimeCol =
                new PhysicalColumn(
                        "updated_time",
                        DataTypes.TIMESTAMP_LTZ(),
                        null,
                        StarRocksUtils.INVALID_OR_MISSING_DATATIME);

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

        runJobWithEvents(events);

        List<String> actual = inspectTableSchema(tableId);

        List<String> expected =
                Arrays.asList(
                        "id | int | NO | true | null",
                        "name | varchar(150) | YES | false | null",
                        "created_time | datetime | YES | false | "
                                + StarRocksUtils.DEFAULT_DATETIME,
                        "updated_time | datetime | YES | false | "
                                + StarRocksUtils.DEFAULT_DATETIME);

        assertEqualsInOrder(expected, actual);
    }
}
