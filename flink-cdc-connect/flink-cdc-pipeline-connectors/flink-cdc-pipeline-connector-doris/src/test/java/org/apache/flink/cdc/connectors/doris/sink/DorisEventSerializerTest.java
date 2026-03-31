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

    private static void serializeObject(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
        }
    }
}
