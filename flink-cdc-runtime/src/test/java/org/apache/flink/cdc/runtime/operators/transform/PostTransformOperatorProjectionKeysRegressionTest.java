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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression tests for primary/partition key names staying consistent with post-projection column
 * names (see FLINK-CDC discussion: Doris DDL must not list {@code id} when the column is {@code
 * ID}).
 */
class PostTransformOperatorProjectionKeysRegressionTest {

    @Test
    void testIcLimitTimeStyleProjectionRemapsPrimaryAndPartitionKeys() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "ic_limit_time");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                        .physicalColumn("CORP_CODE", DataTypes.VARCHAR(32).notNull())
                        .physicalColumn("ACC_TYPE_ID", DataTypes.DECIMAL(20, 0))
                        .physicalColumn("START_TIME", DataTypes.TIME(0).notNull())
                        .physicalColumn("END_TIME", DataTypes.TIME(0).notNull())
                        .physicalColumn("CREATE_BY", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("CREATE_TIME", DataTypes.TIMESTAMP(0).notNull())
                        .physicalColumn("MODIFY_BY", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("MODIFY_TIME", DataTypes.TIMESTAMP(0).notNull())
                        .physicalColumn("DELETED", DataTypes.CHAR(1).notNull())
                        .primaryKey("id")
                        .partitionKey("START_TIME")
                        .build();

        String projection =
                "id AS ID,\n"
                        + "CORP_CODE,\n"
                        + "ACC_TYPE_ID,\n"
                        + "START_TIME,\n"
                        + "END_TIME,\n"
                        + "CREATE_BY,\n"
                        + "CREATE_TIME,\n"
                        + "MODIFY_BY,\n"
                        + "MODIFY_TIME,\n"
                        + "DELETED";

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), projection, null)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.DECIMAL(20, 0).notNull())
                        .physicalColumn("CORP_CODE", DataTypes.VARCHAR(32).notNull())
                        .physicalColumn("ACC_TYPE_ID", DataTypes.DECIMAL(20, 0))
                        .physicalColumn("START_TIME", DataTypes.TIME(0).notNull())
                        .physicalColumn("END_TIME", DataTypes.TIME(0).notNull())
                        .physicalColumn("CREATE_BY", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("CREATE_TIME", DataTypes.TIMESTAMP(0).notNull())
                        .physicalColumn("MODIFY_BY", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("MODIFY_TIME", DataTypes.TIMESTAMP(0).notNull())
                        .physicalColumn("DELETED", DataTypes.CHAR(1).notNull())
                        .primaryKey("ID")
                        .partitionKey("START_TIME")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testWrongExplicitPrimaryKeyNameFallsBackToLineageRemapping() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "ic_limit_time");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id AS ID, name",
                                null,
                                "id",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testUpperCaseFormatRenamesForwardedColumnsAndKeys() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "ic_limit_time");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .partitionKey("name")
                        .build();

        String projection = "id AS keep_id, name";

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                projection,
                                null,
                                "id",
                                "name",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("keep_id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .primaryKey("keep_id")
                        .partitionKey("NAME")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testExplicitProjectionLowerCaseRewritesAlterColumnTypeEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .physicalColumn("flag", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id, JOB",
                                null,
                                "id",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("JOB", DataTypes.VARCHAR(255)),
                                Collections.emptyMap(),
                                Collections.singletonMap("JOB", "job comment"))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.singletonMap("job", DataTypes.VARCHAR(255)),
                                        Collections.emptyMap(),
                                        Collections.singletonMap("job", "job comment"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesRenameColumnEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("JOB", "job_name"))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new RenameColumnEvent(
                                        tableId, Collections.singletonMap("job", "job_name"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatSkipsCaseOnlyRenameColumnEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new RenameColumnEvent(tableId, Collections.singletonMap("JOB", "job"))));

        Assertions.assertThat(harness.getOutputRecords()).isEmpty();
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesUnderscoreRenameColumnEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB_NAME", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("JOB_NAME", "JOB_NAME_123"))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new RenameColumnEvent(
                                        tableId,
                                        Collections.singletonMap("job_name", "job_name_123"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesAlterColumnTypeEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Map<String, String> comments = new HashMap<>();
        comments.put("JOB", "job comment");

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("JOB", DataTypes.VARCHAR(255)),
                                Collections.emptyMap(),
                                comments)));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.singletonMap("job", DataTypes.VARCHAR(255)),
                                        Collections.emptyMap(),
                                        Collections.singletonMap("job", "job comment"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesDuplicatedForwardedColumnsByPostSchemaDiff() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema = Schema.newBuilder().physicalColumn("JOB", DataTypes.STRING()).build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "JOB, JOB AS job_copy",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("JOB", DataTypes.VARCHAR(255)))));

        Map<String, DataType> expectedTypeMapping = new HashMap<>();
        expectedTypeMapping.put("job", DataTypes.VARCHAR(255));
        expectedTypeMapping.put("job_copy", DataTypes.VARCHAR(255));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(new AlterColumnTypeEvent(tableId, expectedTypeMapping)));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesAlterColumnCommentOnlyEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.singletonMap("JOB", "job comment"))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        Collections.singletonMap("job", "job comment"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesDropColumnEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB_NAME", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new DropColumnEvent(tableId, Collections.singletonList("JOB_NAME"))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new DropColumnEvent(
                                        tableId, Collections.singletonList("job_name"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesDropColumnEventAfterCaseOnlyNameResolution() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("job_nam", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("job_nam", "job_name"))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new RenameColumnEvent(
                                        tableId, Collections.singletonMap("job_nam", "job_name"))));

        transform.processElement(
                new StreamRecord<>(
                        new DropColumnEvent(tableId, Collections.singletonList("JOB_NAME"))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new DropColumnEvent(
                                        tableId, Collections.singletonList("job_name"))));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesCommentRemovalAlterColumnTypeEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING(), "old job comment")
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        Map<String, String> comments = new HashMap<>();
        comments.put("JOB", null);
        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                comments)));

        Map<String, String> expectedComments = new HashMap<>();
        expectedComments.put("job", null);
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        expectedComments)));
        harness.close();
    }

    @Test
    void testLowerCaseFormatRewritesAddColumnEvent() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "*",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        harness.getOutputRecords().poll();

        transform.processElement(
                new StreamRecord<>(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("AGE", DataTypes.INT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "JOB")))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AddColumnEvent(
                                        tableId,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "age", DataTypes.INT()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "job")))));
        harness.close();
    }

    @Test
    void testImplicitLowerCaseFormatRewritesSchemaChangeEvents() throws Exception {
        TableId tableId = TableId.tableId("test", "student");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT().notNull())
                        .physicalColumn("JOB", DataTypes.STRING())
                        .physicalColumn("JOB_NAME", DataTypes.STRING())
                        .primaryKey("ID")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                null,
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.LOWER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        tableId,
                                        Schema.newBuilder()
                                                .physicalColumn("id", DataTypes.BIGINT().notNull())
                                                .physicalColumn("job", DataTypes.STRING())
                                                .physicalColumn("job_name", DataTypes.STRING())
                                                .primaryKey("id")
                                                .build())));

        transform.processElement(
                new StreamRecord<>(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("AGE", DataTypes.INT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "JOB")))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AddColumnEvent(
                                        tableId,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "age", DataTypes.INT()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "job")))));

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("JOB", DataTypes.VARCHAR(255)),
                                Collections.emptyMap(),
                                Collections.singletonMap("JOB", "job comment"))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.singletonMap("job", DataTypes.VARCHAR(255)),
                                        Collections.emptyMap(),
                                        Collections.singletonMap("job", "job comment"))));

        transform.processElement(
                new StreamRecord<>(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("JOB", "JOB_TITLE"))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new RenameColumnEvent(
                                        tableId, Collections.singletonMap("job", "job_title"))));

        transform.processElement(
                new StreamRecord<>(
                        new DropColumnEvent(tableId, Collections.singletonList("JOB_NAME"))));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new DropColumnEvent(
                                        tableId, Collections.singletonList("job_name"))));
        harness.close();
    }

    @Test
    void testMysqlTUserWildcardProjectionWithPipelineUpperCaseFormat() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "t_user");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                "(saas_pw_00).t_user",
                                null,
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("ID")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testMysqlTUserExplicitColumnListWithPipelineUpperCaseFormat() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "t_user");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        String projection = "id, NAME, AGE";

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                "(saas_pw_00).t_user",
                                projection,
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("ID")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testPipelineDefaultTableInclusionsUpperCaseMysqlTUser() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "t_user");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS,
                                null,
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("ID")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testExplicitKeysResolveUsingExactAndCaseMatchedNamesTogether() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "ic_limit_time");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id", "name")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id AS keep_id, name",
                                null,
                                "keep_id,name",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("keep_id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING().notNull())
                        .primaryKey("keep_id", "NAME")
                        .build();

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));
        harness.close();
    }

    @Test
    void testAlterColumnTypeEventUsesPostSchemaNamesAfterCaseRewrite() throws Exception {
        TableId tableId = TableId.tableId("saas_pw_00", "t_user");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id, name",
                                null,
                                "",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        tableId,
                                        Schema.newBuilder()
                                                .physicalColumn("ID", DataTypes.BIGINT().notNull())
                                                .physicalColumn("NAME", DataTypes.STRING())
                                                .primaryKey("ID")
                                                .build())));

        transform.processElement(
                new StreamRecord<>(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("name", DataTypes.VARCHAR(17)))));

        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new AlterColumnTypeEvent(
                                        tableId,
                                        Collections.singletonMap("NAME", DataTypes.VARCHAR(17)))));
        harness.close();
    }

    @Test
    void testProjectionAliasesKeepAuthoredCaseWhenPipelineUpperCaseIsEnabled() throws Exception {
        TableId tableId = TableId.tableId("testdb", "customer");
        Schema sourceSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("NAME", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id AS my_id, NAME AS name, age AS AGE",
                                null,
                                "id",
                                "",
                                "",
                                ",",
                                "",
                                new SupportedMetadataColumn[0],
                                SchemaColumnCaseFormat.UPPER)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1);
        harness.open();

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("my_id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("AGE", DataTypes.INT())
                        .primaryKey("my_id")
                        .build();

        transform.processElement(new StreamRecord<>(new CreateTableEvent(tableId, sourceSchema)));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(tableId, expectedSchema)));

        BinaryRecordDataGenerator sourceGenerator =
                new BinaryRecordDataGenerator(((RowType) sourceSchema.toRowDataType()));
        BinaryRecordDataGenerator expectedGenerator =
                new BinaryRecordDataGenerator(((RowType) expectedSchema.toRowDataType()));

        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        tableId,
                        sourceGenerator.generate(
                                new Object[] {1L, BinaryStringData.fromString("Alice"), 18}));
        DataChangeEvent expectedInsertEvent =
                DataChangeEvent.insertEvent(
                        tableId,
                        expectedGenerator.generate(
                                new Object[] {1L, BinaryStringData.fromString("Alice"), 18}));

        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(harness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(expectedInsertEvent));
        harness.close();
    }
}
