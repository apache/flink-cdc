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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE;

/** A test for {@link org.apache.flink.cdc.connectors.doris.sink.DorisEventSerializer} . */
public class DorisEventSerializerTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private static DorisEventSerializer dorisEventSerializer;

    private static final TableId TABLE_ID = TableId.parse("doris_database.doris_table");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("create_date", DataTypes.DATE())
                    .physicalColumn("create_time", DataTypes.TIMESTAMP())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator RECORD_DATA_GENERATOR =
            new BinaryRecordDataGenerator(((RowType) SCHEMA.toRowDataType()));

    @Test
    public void testDataChangeEventWithDateTimePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.parse("2025-01-16T08:00:00Z"), ZoneId.of("Z"));
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo("2025-01-16 08:00:00.000000");
    }

    @Test
    public void testDataChangeEventIfDatetimePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATETIME);
    }

    @Test
    public void testDataChangeEventWithDatePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText()).isEqualTo("2025-01-16");
    }

    @Test
    public void testDataChangeEventIfDatePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    null,
                                    TimestampData.fromMillis(System.currentTimeMillis()),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATE);
    }

    /**
     * TIME is mapped to STRING in Doris DDL; row values must be JSON-serializable strings (not
     * {@code java.time.LocalTime}, which default Jackson cannot serialize).
     */
    @Test
    public void testDataChangeEventWithTimeColumn() throws IOException {
        TableId timeTableId = TableId.parse("doris_database.time_table");
        Schema timeSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("start_time", DataTypes.TIME(3))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator timeGen =
                new BinaryRecordDataGenerator(((RowType) timeSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(timeTableId, timeSchema));

        DataChangeEvent insert =
                DataChangeEvent.insertEvent(
                        timeTableId,
                        timeGen.generate(
                                new Object[] {
                                    1,
                                    // 01:01:01.123 — same nanos as DorisRowConverterTest
                                    TimeData.fromNanoOfDay(3661123_000000L)
                                }));

        DorisRecord dorisRecord = serializer.serialize(insert);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("start_time").asText()).isEqualTo("01:01:01.123");
    }

    @Test
    public void testDataChangeEventWithCsvFormat() throws IOException {
        DorisEventSerializer serializer =
                createCsvSerializer((tableId, inputSchema) -> inputSchema);
        serializer.serialize(new CreateTableEvent(TABLE_ID, SCHEMA));

        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.parse("2025-01-16T08:00:00Z"), ZoneId.of("Z"));
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord insertRecord = serializer.serialize(insertEvent);
        Assertions.assertThat(new String(insertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("1|flink|2025-01-16|2025-01-16 08:00:00.000000|0");

        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord deleteRecord = serializer.serialize(deleteEvent);
        Assertions.assertThat(new String(deleteRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("1|flink|2025-01-16|2025-01-16 08:00:00.000000|1");
    }

    @Test
    public void testDataChangeEventWithCsvFormatUsesPhysicalColumnOrder() throws IOException {
        DorisEventSerializer serializer =
                createCsvSerializer(
                        (tableId, inputSchema) ->
                                inputSchema.copy(
                                        Arrays.asList(
                                                inputSchema
                                                        .getColumn("name")
                                                        .orElseThrow(IllegalStateException::new),
                                                inputSchema
                                                        .getColumn("id")
                                                        .orElseThrow(IllegalStateException::new),
                                                inputSchema
                                                        .getColumn("create_time")
                                                        .orElseThrow(IllegalStateException::new),
                                                inputSchema
                                                        .getColumn("create_date")
                                                        .orElseThrow(IllegalStateException::new))));
        serializer.serialize(new CreateTableEvent(TABLE_ID, SCHEMA));

        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.parse("2025-01-16T08:00:00Z"), ZoneId.of("Z"));
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord insertRecord = serializer.serialize(insertEvent);
        Assertions.assertThat(new String(insertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("flink|1|2025-01-16 08:00:00.000000|2025-01-16|0");
    }

    @Test
    public void testDataChangeEventWithCsvFormatUsesExplicitColumnsOrder() throws IOException {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        streamLoadProps.setProperty("column_separator", "|");
        streamLoadProps.setProperty("columns", "`name`,`id`");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration(), executionOptions);
        serializer.serialize(new CreateTableEvent(TABLE_ID, SCHEMA));

        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromMillis(System.currentTimeMillis()),
                                }));

        DorisRecord insertRecord = serializer.serialize(insertEvent);
        Assertions.assertThat(new String(insertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("flink|1|0");
    }

    @Test
    public void testCsvFormatRejectsColumnsExpressions() {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        streamLoadProps.setProperty("column_separator", "|");
        streamLoadProps.setProperty("columns", "`id`,tmp_name=upper(`name`)");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration(), executionOptions);

        Assertions.assertThatThrownBy(
                        () -> serializer.serialize(new CreateTableEvent(TABLE_ID, SCHEMA)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not support expressions");
    }

    @Test
    public void testCsvSerializerWithExplicitColumnsIsSerializable() throws IOException {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        streamLoadProps.setProperty("column_separator", "|");
        streamLoadProps.setProperty("columns", "`name`,`id`");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration(), executionOptions);

        serializeObject(serializer);
    }

    @Test
    public void testCsvSerializerWithDorisSchemaResolverIsSerializable() throws IOException {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setUsername("root")
                        .setPassword("")
                        .build();

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"), new Configuration(), executionOptions, dorisOptions);

        serializeObject(serializer);
    }

    @Test
    public void testSchemaChangeRefreshesCsvPhysicalColumnOrder() throws IOException {
        TableId schemaChangeTableId = TableId.parse("doris_database.schema_change_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator baseRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) baseSchema.toRowDataType()));

        DorisEventSerializer serializer =
                createCsvSerializer(
                        (tableId, inputSchema) ->
                                inputSchema.getColumn("age").isPresent()
                                        ? inputSchema.copy(
                                                Arrays.asList(
                                                        inputSchema
                                                                .getColumn("id")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("name")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("age")
                                                                .orElseThrow(
                                                                        IllegalStateException
                                                                                ::new)))
                                        : inputSchema);
        serializer.serialize(new CreateTableEvent(schemaChangeTableId, baseSchema));

        DorisRecord baseInsertRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                schemaChangeTableId,
                                baseRecordDataGenerator.generate(
                                        new Object[] {
                                            new BinaryStringData("1"), new BinaryStringData("flink")
                                        })));
        Assertions.assertThat(new String(baseInsertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("1|flink|0");

        AddColumnEvent addAgeFirstEvent =
                new AddColumnEvent(
                        schemaChangeTableId,
                        Arrays.asList(
                                AddColumnEvent.first(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        serializer.serialize(addAgeFirstEvent);

        Schema evolvedInputSchema =
                SchemaUtils.applySchemaChangeEvent(baseSchema, addAgeFirstEvent);
        BinaryRecordDataGenerator evolvedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedInputSchema.toRowDataType()));
        DorisRecord evolvedInsertRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                schemaChangeTableId,
                                evolvedRecordDataGenerator.generate(
                                        new Object[] {
                                            18,
                                            new BinaryStringData("2"),
                                            new BinaryStringData("stream")
                                        })));
        Assertions.assertThat(new String(evolvedInsertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("2|stream|18|0");
    }

    @Test
    public void testCreateTableEventRefreshesJsonSchemaAfterRestoreWithComputedTimestampColumn()
            throws IOException {
        TableId restoreTableId = TableId.parse("doris_database.restore_timestamp_table");
        Schema restoredSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.STRING())
                        .build();
        Schema expandedSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.STRING())
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP_LTZ(3))
                        .build();
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) expandedSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());

        // Simulate restoring the serializer state with an old schema, then receiving the new
        // CreateTableEvent produced by an updated transform.
        serializer.serialize(new CreateTableEvent(restoreTableId, restoredSchema));
        serializer.serialize(new CreateTableEvent(restoreTableId, expandedSchema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                restoreTableId,
                                recordDataGenerator.generate(
                                        new Object[] {
                                            1,
                                            1001L,
                                            BinaryStringData.fromString("demo_job"),
                                            LocalZonedTimestampData.fromEpochMillis(
                                                    Instant.parse("2025-01-16T08:00:00Z")
                                                            .toEpochMilli())
                                        })));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("deploy_mode").asInt()).isEqualTo(1);
        Assertions.assertThat(jsonNode.get("project_id").asLong()).isEqualTo(1001L);
        Assertions.assertThat(jsonNode.get("job_name").asText()).isEqualTo("demo_job");
        Assertions.assertThat(jsonNode.get("confluent__last_updated").asText())
                .isEqualTo("2025-01-16 08:00:00.000000");
    }

    @Test
    public void testRenameColumnRefreshesCsvPhysicalColumnNames() throws IOException {
        TableId renameTableId = TableId.parse("doris_database.rename_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        DorisEventSerializer serializer =
                createCsvSerializer(
                        (tableId, inputSchema) ->
                                inputSchema.getColumn("alias").isPresent()
                                        ? inputSchema.copy(
                                                Arrays.asList(
                                                        inputSchema
                                                                .getColumn("alias")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("id")
                                                                .orElseThrow(
                                                                        IllegalStateException
                                                                                ::new)))
                                        : inputSchema);
        serializer.serialize(new CreateTableEvent(renameTableId, baseSchema));

        Map<String, String> nameMapping = new LinkedHashMap<>();
        nameMapping.put("name", "alias");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(renameTableId, nameMapping);
        serializer.serialize(renameColumnEvent);

        Schema evolvedInputSchema =
                SchemaUtils.applySchemaChangeEvent(baseSchema, renameColumnEvent);
        BinaryRecordDataGenerator evolvedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedInputSchema.toRowDataType()));
        DorisRecord evolvedInsertRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                renameTableId,
                                evolvedRecordDataGenerator.generate(
                                        new Object[] {
                                            new BinaryStringData("1"), new BinaryStringData("neo")
                                        })));
        Assertions.assertThat(new String(evolvedInsertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("neo|1|0");
    }

    @Test
    public void testDropColumnRefreshesCsvPhysicalColumns() throws IOException {
        TableId dropColumnTableId = TableId.parse("doris_database.drop_column_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        DorisEventSerializer serializer =
                createCsvSerializer(
                        (tableId, inputSchema) ->
                                inputSchema.getColumn("age").isPresent()
                                        ? inputSchema.copy(
                                                Arrays.asList(
                                                        inputSchema
                                                                .getColumn("name")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("id")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("age")
                                                                .orElseThrow(
                                                                        IllegalStateException
                                                                                ::new)))
                                        : inputSchema.copy(
                                                Arrays.asList(
                                                        inputSchema
                                                                .getColumn("name")
                                                                .orElseThrow(
                                                                        IllegalStateException::new),
                                                        inputSchema
                                                                .getColumn("id")
                                                                .orElseThrow(
                                                                        IllegalStateException
                                                                                ::new))));
        serializer.serialize(new CreateTableEvent(dropColumnTableId, baseSchema));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(dropColumnTableId, Arrays.asList("age"));
        serializer.serialize(dropColumnEvent);

        Schema evolvedInputSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, dropColumnEvent);
        BinaryRecordDataGenerator evolvedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedInputSchema.toRowDataType()));
        DorisRecord evolvedInsertRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                dropColumnTableId,
                                evolvedRecordDataGenerator.generate(
                                        new Object[] {
                                            new BinaryStringData("2"),
                                            new BinaryStringData("trinity")
                                        })));
        Assertions.assertThat(new String(evolvedInsertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("trinity|2|0");
    }

    @Test
    public void testDropColumnKeepsCsvDeleteCompatibleWithOldRowImage() throws IOException {
        TableId dropColumnTableId = TableId.parse("doris_database.drop_column_delete_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("create_time", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        streamLoadProps.setProperty("column_separator", "|");
        streamLoadProps.setProperty("columns", "`name`,`id`");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration(), executionOptions);
        serializer.serialize(new CreateTableEvent(dropColumnTableId, baseSchema));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(dropColumnTableId, Arrays.asList("create_time"));
        serializer.serialize(dropColumnEvent);

        BinaryRecordDataGenerator oldRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) baseSchema.toRowDataType()));
        DorisRecord deleteRecord =
                serializer.serialize(
                        DataChangeEvent.deleteEvent(
                                dropColumnTableId,
                                oldRecordDataGenerator.generate(
                                        new Object[] {
                                            new BinaryStringData("1"),
                                            new BinaryStringData("2025-03-24 03:56:11"),
                                            new BinaryStringData("neo")
                                        })));
        Assertions.assertThat(new String(deleteRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("neo|1|1");
    }

    @Test
    public void testDropColumnKeepsJsonDeleteCompatibleWithOldRowImage() throws IOException {
        TableId dropColumnTableId = TableId.parse("doris_database.drop_column_json_delete_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("create_time", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(dropColumnTableId, baseSchema));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(dropColumnTableId, Arrays.asList("create_time"));
        serializer.serialize(dropColumnEvent);

        BinaryRecordDataGenerator oldRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) baseSchema.toRowDataType()));
        DorisRecord deleteRecord =
                serializer.serialize(
                        DataChangeEvent.deleteEvent(
                                dropColumnTableId,
                                oldRecordDataGenerator.generate(
                                        new Object[] {
                                            new BinaryStringData("2"),
                                            new BinaryStringData("2025-03-24 03:56:24"),
                                            new BinaryStringData("trinity")
                                        })));

        JsonNode jsonNode = objectMapper.readTree(deleteRecord.getRow());
        Assertions.assertThat(jsonNode.get("id").asText()).isEqualTo("2");
        Assertions.assertThat(jsonNode.get("name").asText()).isEqualTo("trinity");
        Assertions.assertThat(jsonNode.has("create_time")).isFalse();
        Assertions.assertThat(jsonNode.get("__DORIS_DELETE_SIGN__").asText()).isEqualTo("1");
    }

    @Test
    public void testAlterColumnTypeRefreshesCsvSchema() throws IOException {
        TableId alterTypeTableId = TableId.parse("doris_database.alter_type_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        DorisEventSerializer serializer =
                createCsvSerializer((tableId, inputSchema) -> inputSchema);
        serializer.serialize(new CreateTableEvent(alterTypeTableId, baseSchema));

        Map<String, org.apache.flink.cdc.common.types.DataType> typeMapping = new LinkedHashMap<>();
        typeMapping.put("id", DataTypes.BIGINT());
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(alterTypeTableId, typeMapping);
        serializer.serialize(alterColumnTypeEvent);

        Schema evolvedInputSchema =
                SchemaUtils.applySchemaChangeEvent(baseSchema, alterColumnTypeEvent);
        BinaryRecordDataGenerator evolvedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedInputSchema.toRowDataType()));
        DorisRecord evolvedInsertRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                alterTypeTableId,
                                evolvedRecordDataGenerator.generate(
                                        new Object[] {1L, new BinaryStringData("morpheus")})));
        Assertions.assertThat(new String(evolvedInsertRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("1|morpheus|0");
    }

    @Test
    public void testJsonSerializationNormalizesUppercaseColumnNamesForDoris() throws IOException {
        TableId uppercaseTableId = TableId.parse("doris_database.uppercase_table");
        Schema uppercaseSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) uppercaseSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(uppercaseTableId, uppercaseSchema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                uppercaseTableId,
                                generator.generate(
                                        new Object[] {1, BinaryStringData.fromString("flink")})));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("id").asInt()).isEqualTo(1);
        Assertions.assertThat(jsonNode.get("name").asText()).isEqualTo("flink");
        Assertions.assertThat(jsonNode.has("ID")).isFalse();
        Assertions.assertThat(jsonNode.has("NAME")).isFalse();
    }

    @Test
    public void testJsonSerializationNormalizesMixedCaseColumnNamesForDoris() throws IOException {
        TableId mixedCaseTableId = TableId.parse("doris_database.mixed_case_orders");
        Schema mixedCaseSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT())
                        .physicalColumn("park_id", DataTypes.INT())
                        .physicalColumn("park_code", DataTypes.STRING())
                        .physicalColumn("order_no", DataTypes.STRING())
                        .physicalColumn("CARD_ID", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) mixedCaseSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(mixedCaseTableId, mixedCaseSchema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                mixedCaseTableId,
                                generator.generate(
                                        new Object[] {
                                            1,
                                            101,
                                            BinaryStringData.fromString("PARK-001"),
                                            BinaryStringData.fromString("ORDER-001"),
                                            BinaryStringData.fromString("CARD-001")
                                        })));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("id").asInt()).isEqualTo(1);
        Assertions.assertThat(jsonNode.get("park_id").asInt()).isEqualTo(101);
        Assertions.assertThat(jsonNode.get("park_code").asText()).isEqualTo("PARK-001");
        Assertions.assertThat(jsonNode.get("order_no").asText()).isEqualTo("ORDER-001");
        Assertions.assertThat(jsonNode.get("card_id").asText()).isEqualTo("CARD-001");
        Assertions.assertThat(jsonNode.has("ID")).isFalse();
        Assertions.assertThat(jsonNode.has("CARD_ID")).isFalse();
    }

    @Test
    public void testJsonSerializationMatchesAgreementProductColumnsReportedByDorisErrorLog()
            throws IOException {
        TableId tableId = TableId.parse("xxsc.agreement_product");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT())
                        .physicalColumn("AGREEMENT_CODE", DataTypes.STRING())
                        .physicalColumn("PARENT_CODE", DataTypes.STRING())
                        .physicalColumn("PARENT_NAME", DataTypes.STRING())
                        .physicalColumn("PRODUCT_CODE", DataTypes.STRING())
                        .physicalColumn("PRODUCT_NAME", DataTypes.STRING())
                        .physicalColumn("PRODUCT_TYPE", DataTypes.STRING())
                        .physicalColumn("ENABLED", DataTypes.STRING())
                        .physicalColumn("STATUS", DataTypes.STRING())
                        .physicalColumn("CREATE_TIME", DataTypes.STRING())
                        .physicalColumn("CREATE_BY", DataTypes.STRING())
                        .physicalColumn("MODIFY_TIME", DataTypes.STRING())
                        .physicalColumn("MODIFY_BY", DataTypes.STRING())
                        .physicalColumn("LAST_UPDATE_TIME", DataTypes.STRING())
                        .physicalColumn("DELETED", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(tableId, schema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            279,
                                            BinaryStringData.fromString("AGCNC24053011045133891"),
                                            BinaryStringData.fromString("park2024031513535177487"),
                                            BinaryStringData.fromString("常州中华恐龙园旅游度假区"),
                                            BinaryStringData.fromString("MP2024052915532228596"),
                                            BinaryStringData.fromString(
                                                    "旅订恐儿童二日二次免票+水一次票（1.2米-1.5米）"),
                                            BinaryStringData.fromString("park"),
                                            BinaryStringData.fromString("T"),
                                            BinaryStringData.fromString("T"),
                                            BinaryStringData.fromString(
                                                    "2024-05-30 11:06:21.000000"),
                                            BinaryStringData.fromString("liuerling"),
                                            BinaryStringData.fromString(
                                                    "2024-06-13 08:59:41.000000"),
                                            BinaryStringData.fromString("liuerling"),
                                            null,
                                            BinaryStringData.fromString("T")
                                        })));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("id").asInt()).isEqualTo(279);
        Assertions.assertThat(jsonNode.get("agreement_code").asText())
                .isEqualTo("AGCNC24053011045133891");
        Assertions.assertThat(jsonNode.get("parent_code").asText())
                .isEqualTo("park2024031513535177487");
        Assertions.assertThat(jsonNode.get("product_code").asText())
                .isEqualTo("MP2024052915532228596");
        Assertions.assertThat(jsonNode.get("deleted").asText()).isEqualTo("T");
        Assertions.assertThat(jsonNode.get("__DORIS_DELETE_SIGN__").asText()).isEqualTo("0");
        Assertions.assertThat(jsonNode.has("ID")).isFalse();
        Assertions.assertThat(jsonNode.has("AGREEMENT_CODE")).isFalse();
        Assertions.assertThat(jsonNode.has("PRODUCT_CODE")).isFalse();
    }

    @Test
    public void testJsonSerializationUsesDorisPhysicalColumnCaseForUppercasePrimaryKey()
            throws IOException {
        TableId tableId = TableId.parse("streampark.t_flink_app");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT())
                        .physicalColumn("deploy_mode", DataTypes.TINYINT())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        (resolvedTableId, inputSchema) -> {
                            LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
                            mapping.put("ID", "ID");
                            mapping.put("deploy_mode", "deploy_mode");
                            return mapping;
                        });
        serializer.serialize(new CreateTableEvent(tableId, schema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId, generator.generate(new Object[] {1L, (byte) 2})));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("ID").asLong()).isEqualTo(1L);
        Assertions.assertThat(jsonNode.get("deploy_mode").asInt()).isEqualTo(2);
        Assertions.assertThat(jsonNode.has("id")).isFalse();
    }

    @Test
    public void testJsonSerializationUsesUppercaseDorisColumnsForUpperCaseFormattedSchema()
            throws IOException {
        TableId tableId = TableId.parse("streampark.t_flink_app_upper");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT())
                        .physicalColumn("DEPLOY_MODE", DataTypes.TINYINT())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        (resolvedTableId, inputSchema) -> {
                            LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
                            mapping.put("ID", "ID");
                            mapping.put("DEPLOY_MODE", "DEPLOY_MODE");
                            return mapping;
                        });
        serializer.serialize(new CreateTableEvent(tableId, schema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId, generator.generate(new Object[] {1L, (byte) 2})));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("ID").asLong()).isEqualTo(1L);
        Assertions.assertThat(jsonNode.get("DEPLOY_MODE").asInt()).isEqualTo(2);
        Assertions.assertThat(jsonNode.has("id")).isFalse();
        Assertions.assertThat(jsonNode.has("deploy_mode")).isFalse();
    }

    @Test
    public void testJsonSerializationUsesExistingLowercaseDorisColumnsForUppercaseSourceColumns()
            throws IOException {
        TableId tableId = TableId.parse("xxsc.agreement_product_existing");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT())
                        .physicalColumn("AGREEMENT_CODE", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        (resolvedTableId, inputSchema) -> {
                            LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
                            mapping.put("id", "ID");
                            mapping.put("agreement_code", "AGREEMENT_CODE");
                            return mapping;
                        });
        serializer.serialize(new CreateTableEvent(tableId, schema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            279,
                                            BinaryStringData.fromString("AGCNC24053011045133891")
                                        })));

        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("id").asInt()).isEqualTo(279);
        Assertions.assertThat(jsonNode.get("agreement_code").asText())
                .isEqualTo("AGCNC24053011045133891");
        Assertions.assertThat(jsonNode.has("ID")).isFalse();
        Assertions.assertThat(jsonNode.has("AGREEMENT_CODE")).isFalse();
    }

    @Test
    public void testJsonSerializationHandlesLateEventsAfterAddColumn() throws IOException {
        TableId tableId = TableId.parse("doris_database.json_schema_late_events");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator baseGenerator =
                new BinaryRecordDataGenerator(((RowType) baseSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        (resolvedTableId, inputSchema) -> {
                            LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
                            mapping.put("id", "id");
                            mapping.put("name", "name");
                            mapping.put(
                                    "age",
                                    inputSchema.getColumn("age").map(Column::getName).orElse(null));
                            return mapping;
                        });
        serializer.serialize(new CreateTableEvent(tableId, baseSchema));

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        serializer.serialize(addColumnEvent);

        DorisRecord lateRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId,
                                baseGenerator.generate(
                                        new Object[] {1, BinaryStringData.fromString("Alice")})));

        JsonNode lateJsonNode = objectMapper.readTree(lateRecord.getRow());
        Assertions.assertThat(lateJsonNode.get("id").asInt()).isEqualTo(1);
        Assertions.assertThat(lateJsonNode.get("name").asText()).isEqualTo("Alice");
        Assertions.assertThat(lateJsonNode.get("age").isNull()).isTrue();

        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);
        BinaryRecordDataGenerator evolvedGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedSchema.toRowDataType()));
        DorisRecord evolvedRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId,
                                evolvedGenerator.generate(
                                        new Object[] {2, BinaryStringData.fromString("Bob"), 18})));

        JsonNode evolvedJsonNode = objectMapper.readTree(evolvedRecord.getRow());
        Assertions.assertThat(evolvedJsonNode.get("id").asInt()).isEqualTo(2);
        Assertions.assertThat(evolvedJsonNode.get("name").asText()).isEqualTo("Bob");
        Assertions.assertThat(evolvedJsonNode.get("age").asInt()).isEqualTo(18);
    }

    @Test
    public void testJsonSerializationRetriesUntilDorisSchemaCatchesUp() throws IOException {
        TableId tableId = TableId.parse("doris_database.json_schema_retry_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);
        BinaryRecordDataGenerator evolvedGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedSchema.toRowDataType()));
        AtomicInteger fetchAttempts = new AtomicInteger();

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        DorisEventSerializer.createJsonFieldMappingResolver(
                                createDorisOptions(),
                                (options, requestedTableId) -> {
                                    int attempt = fetchAttempts.incrementAndGet();
                                    return attempt < 3
                                            ? createDorisSchema("id", "name")
                                            : createDorisSchema("id", "name", "age");
                                },
                                100L,
                                1L));
        serializer.serialize(new CreateTableEvent(tableId, baseSchema));
        serializer.serialize(addColumnEvent);

        DorisRecord evolvedRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                tableId,
                                evolvedGenerator.generate(
                                        new Object[] {2, BinaryStringData.fromString("Bob"), 18})));

        JsonNode evolvedJsonNode = objectMapper.readTree(evolvedRecord.getRow());
        Assertions.assertThat(fetchAttempts.get()).isEqualTo(3);
        Assertions.assertThat(evolvedJsonNode.get("id").asInt()).isEqualTo(2);
        Assertions.assertThat(evolvedJsonNode.get("name").asText()).isEqualTo("Bob");
        Assertions.assertThat(evolvedJsonNode.get("age").asInt()).isEqualTo(18);
    }

    @Test
    public void testWaitUntilDorisSchemaCoversInputSchemaWithinTimeBudget() {
        TableId tableId = TableId.parse("doris_database.json_schema_time_budget_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);
        AtomicInteger fetchAttempts = new AtomicInteger();

        org.apache.doris.flink.rest.models.Schema dorisSchema =
                DorisEventSerializer.waitUntilDorisSchemaCoversInputSchema(
                        createDorisOptions(),
                        tableId,
                        evolvedSchema,
                        (options, requestedTableId) -> {
                            int attempt = fetchAttempts.incrementAndGet();
                            return attempt < 6
                                    ? createDorisSchema("id", "name")
                                    : createDorisSchema("id", "name", "age");
                        },
                        100L,
                        1L);

        Assertions.assertThat(fetchAttempts.get()).isEqualTo(6);
        Assertions.assertThat(dorisSchema.getProperties())
                .extracting(org.apache.doris.flink.rest.models.Field::getName)
                .containsExactly("id", "name", "age");
    }

    @Test
    public void testWaitUntilDorisSchemaCoversInputSchemaTimesOut() {
        TableId tableId = TableId.parse("doris_database.json_schema_timeout_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);

        Assertions.assertThatThrownBy(
                        () ->
                                DorisEventSerializer.waitUntilDorisSchemaCoversInputSchema(
                                        createDorisOptions(),
                                        tableId,
                                        evolvedSchema,
                                        (options, requestedTableId) ->
                                                createDorisSchema("id", "name"),
                                        10L,
                                        1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not catch up with input schema");
    }

    @Test
    public void testWaitUntilDorisSchemaCoversInputSchemaRetriesFetchFailuresWithinTimeBudget() {
        TableId tableId = TableId.parse("doris_database.json_schema_fetch_retry_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);
        AtomicInteger fetchAttempts = new AtomicInteger();

        org.apache.doris.flink.rest.models.Schema dorisSchema =
                DorisEventSerializer.waitUntilDorisSchemaCoversInputSchema(
                        createDorisOptions(),
                        tableId,
                        evolvedSchema,
                        (options, requestedTableId) -> {
                            int attempt = fetchAttempts.incrementAndGet();
                            if (attempt < 3) {
                                throw new IllegalStateException("schema fetch failed");
                            }
                            return createDorisSchema("id", "name", "age");
                        },
                        100L,
                        1L);

        Assertions.assertThat(fetchAttempts.get()).isEqualTo(3);
        Assertions.assertThat(dorisSchema.getProperties())
                .extracting(org.apache.doris.flink.rest.models.Field::getName)
                .containsExactly("id", "name", "age");
    }

    @Test
    public void testWaitUntilDorisSchemaCoversInputSchemaFailsAfterRepeatedFetchFailures() {
        TableId tableId = TableId.parse("doris_database.json_schema_fetch_timeout_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);

        Assertions.assertThatThrownBy(
                        () ->
                                DorisEventSerializer.waitUntilDorisSchemaCoversInputSchema(
                                        createDorisOptions(),
                                        tableId,
                                        evolvedSchema,
                                        (options, requestedTableId) -> {
                                            throw new IllegalStateException("schema fetch failed");
                                        },
                                        10L,
                                        1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to resolve Doris physical schema")
                .hasRootCauseMessage("schema fetch failed");
    }

    @Test
    public void testWaitUntilDorisSchemaCoversInputSchemaPreservesInterrupt() {
        TableId tableId = TableId.parse("doris_database.json_schema_interrupt_table");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);

        Thread.currentThread().interrupt();
        try {
            Assertions.assertThatThrownBy(
                            () ->
                                    DorisEventSerializer.waitUntilDorisSchemaCoversInputSchema(
                                            createDorisOptions(),
                                            tableId,
                                            evolvedSchema,
                                            (options, requestedTableId) ->
                                                    createDorisSchema("id", "name"),
                                            100L,
                                            50L))
                    .isInstanceOf(org.apache.flink.util.FlinkRuntimeException.class)
                    .hasMessageContaining("Interrupted while waiting for Doris physical schema");
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testJsonSerializationPropagatesDorisSchemaFetchFailure() throws IOException {
        TableId tableId = TableId.parse("doris_database.json_schema_fetch_failure");
        Schema baseSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT()))));
        Schema evolvedSchema = SchemaUtils.applySchemaChangeEvent(baseSchema, addColumnEvent);
        BinaryRecordDataGenerator evolvedGenerator =
                new BinaryRecordDataGenerator(((RowType) evolvedSchema.toRowDataType()));

        DorisEventSerializer serializer =
                new DorisEventSerializer(
                        ZoneId.of("UTC"),
                        new Configuration(),
                        DorisExecutionOptions.defaults(),
                        null,
                        DorisEventSerializer.createJsonFieldMappingResolver(
                                createDorisOptions(),
                                (options, requestedTableId) -> {
                                    throw new IllegalStateException("schema fetch failed");
                                },
                                10L,
                                1L));
        serializer.serialize(new CreateTableEvent(tableId, baseSchema));
        serializer.serialize(addColumnEvent);

        Assertions.assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        DataChangeEvent.insertEvent(
                                                tableId,
                                                evolvedGenerator.generate(
                                                        new Object[] {
                                                            2,
                                                            BinaryStringData.fromString("Bob"),
                                                            18
                                                        }))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to resolve Doris physical schema")
                .hasRootCauseMessage("schema fetch failed");
    }

    @Test
    public void testCsvSerializationPreservesMixedCaseColumnValues() throws IOException {
        TableId mixedCaseTableId = TableId.parse("doris_database.mixed_case_orders_csv");
        Schema mixedCaseSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT())
                        .physicalColumn("park_id", DataTypes.INT())
                        .physicalColumn("park_code", DataTypes.STRING())
                        .physicalColumn("order_no", DataTypes.STRING())
                        .physicalColumn("CARD_ID", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) mixedCaseSchema.toRowDataType()));

        DorisEventSerializer serializer =
                createCsvSerializer((tableId, inputSchema) -> inputSchema);
        serializer.serialize(new CreateTableEvent(mixedCaseTableId, mixedCaseSchema));

        DorisRecord dorisRecord =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                mixedCaseTableId,
                                generator.generate(
                                        new Object[] {
                                            1,
                                            101,
                                            BinaryStringData.fromString("PARK-001"),
                                            BinaryStringData.fromString("ORDER-001"),
                                            BinaryStringData.fromString("CARD-001")
                                        })));

        Assertions.assertThat(new String(dorisRecord.getRow(), StandardCharsets.UTF_8))
                .isEqualTo("1|101|PARK-001|ORDER-001|CARD-001|0");
    }

    private static DorisEventSerializer createCsvSerializer(
            DorisEventSerializer.CsvOutputSchemaResolver csvOutputSchemaResolver) {
        return new DorisEventSerializer(
                ZoneId.of("UTC"),
                new Configuration(),
                createCsvExecutionOptions(),
                csvOutputSchemaResolver);
    }

    private static DorisExecutionOptions createCsvExecutionOptions() {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "csv");
        streamLoadProps.setProperty("column_separator", "|");
        return DorisExecutionOptions.builder().setStreamLoadProp(streamLoadProps).build();
    }

    private static DorisOptions createDorisOptions() {
        return DorisOptions.builder()
                .setFenodes("127.0.0.1:8030")
                .setUsername("root")
                .setPassword("")
                .build();
    }

    private static org.apache.doris.flink.rest.models.Schema createDorisSchema(String... columns) {
        org.apache.doris.flink.rest.models.Schema dorisSchema =
                new org.apache.doris.flink.rest.models.Schema(columns.length);
        for (String column : columns) {
            dorisSchema.put(
                    new org.apache.doris.flink.rest.models.Field(
                            column, "VARCHAR", null, 0, 0, null));
        }
        return dorisSchema;
    }

    private static void serializeObject(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
        }
    }
}
