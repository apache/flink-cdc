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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for table-buckets parsing in {@link DorisMetadataApplier}. */
public class DorisMetadataApplierTest {

    private static final TableId TABLE_ID = TableId.parse("streampark.t_flink_app");

    @Test
    public void testParseTableBucketsNotConfigured() {
        Configuration config = Configuration.fromMap(Collections.emptyMap());
        Map<String, Integer> result = DorisMetadataApplier.parseTableBuckets(config);
        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testParseTableBucketsSingleTable() {
        Configuration config =
                Configuration.fromMap(Collections.singletonMap("table-buckets", "orders:10"));
        Map<String, Integer> result = DorisMetadataApplier.parseTableBuckets(config);
        Assertions.assertThat(result).hasSize(1).containsEntry("orders", 10);
    }

    @Test
    public void testParseTableBucketsMultipleTables() {
        Configuration config =
                Configuration.fromMap(
                        Collections.singletonMap(
                                "table-buckets", "tbl1:10,tbl2 : 20, a.* :30,b.*:40,.*:50"));
        Map<String, Integer> result = DorisMetadataApplier.parseTableBuckets(config);
        Assertions.assertThat(result)
                .hasSize(5)
                .containsEntry("tbl1", 10)
                .containsEntry("tbl2", 20)
                .containsEntry("a.*", 30)
                .containsEntry("b.*", 40)
                .containsEntry(".*", 50);
    }

    @Test
    public void testParseTableBucketsPreservesOrder() {
        Configuration config =
                Configuration.fromMap(
                        Collections.singletonMap("table-buckets", "first:1,second:2,third:3"));
        Map<String, Integer> result = DorisMetadataApplier.parseTableBuckets(config);
        Assertions.assertThat(result).isInstanceOf(LinkedHashMap.class);
        Assertions.assertThat(result.keySet()).containsExactly("first", "second", "third");
    }

    @Test
    public void testTableBucketsExactMatch() {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        bucketsMap.put("orders", 10);
        bucketsMap.put("users", 20);
        bucketsMap.put(".*", 6);

        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "orders"))
                .isEqualTo(10);
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "users"))
                .isEqualTo(20);
    }

    @Test
    public void testTableBucketsRegexMatch() {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        bucketsMap.put("order.*", 10);
        bucketsMap.put("user.*", 20);

        Assertions.assertThat(
                        DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "order_detail"))
                .isEqualTo(10);
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "user_info"))
                .isEqualTo(20);
    }

    @Test
    public void testTableBucketsWildcardMatchAll() {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        bucketsMap.put(".*", 6);

        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "any_table"))
                .isEqualTo(6);
        Assertions.assertThat(
                        DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "another_table"))
                .isEqualTo(6);
    }

    @Test
    public void testTableBucketsNoMatch() {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        bucketsMap.put("orders", 10);

        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "users"))
                .isNull();
    }

    @Test
    public void testTableBucketsExactMatchPriority() {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        bucketsMap.put("orders", 10);
        bucketsMap.put("order.*", 20);
        bucketsMap.put(".*", 30);

        // Exact match takes priority over regex
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "orders"))
                .isEqualTo(10);
        // Regex match
        Assertions.assertThat(
                        DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "order_detail"))
                .isEqualTo(20);
        // Wildcard fallback
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "users"))
                .isEqualTo(30);
    }

    @Test
    public void testTableBucketsEmptyMap() {
        Assertions.assertThat(
                        DorisSchemaFactory.parseTableSchemaBuckets(
                                Collections.emptyMap(), "orders"))
                .isNull();
    }

    @Test
    public void testTableBucketsNullMap() {
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(null, "orders")).isNull();
    }

    @Test
    public void testParseTableBucketsEndToEnd() {
        // Simulate the full flow: config -> parse -> resolve
        Configuration config =
                Configuration.fromMap(
                        Collections.singletonMap("table-buckets", "orders:10,user.*:20,.*:6"));
        Map<String, Integer> bucketsMap = DorisMetadataApplier.parseTableBuckets(config);

        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "orders"))
                .isEqualTo(10);
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "user_info"))
                .isEqualTo(20);
        Assertions.assertThat(DorisSchemaFactory.parseTableSchemaBuckets(bucketsMap, "products"))
                .isEqualTo(6);
    }

    @Test
    public void testCreateTableEventReconcilesMissingColumnsForExistingTable() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("DEPLOY_MODE", "INT"),
                                dorisField("project_id", "BIGINT"),
                                dorisVarcharField("job_name", 51)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.VARCHAR(17))
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnName)
                .isEqualTo("CONFLUENT__LAST_UPDATED");
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnType)
                .isEqualTo("DATETIMEV2(3)");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly(
                        "DEPLOY_MODE", "project_id", "job_name", "CONFLUENT__LAST_UPDATED");
    }

    @Test
    public void testCreateTableEventCachesExistingDorisPhysicalColumnOrder() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("score", "INT"),
                                dorisField("name", "STRING")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("score", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "score", "name");

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("age", DataTypes.INT()), "name"))));

        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "age", "name", "score");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "score", "age", "name");
        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("score");
    }

    @Test
    public void testCreateTableEventAppliesExtraSchemaOnlyForNewTable() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put(
                "table.create.extra-schema",
                "`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),"
                        + "`_db_` STRING NOT NULL,"
                        + "`_tb_` STRING NOT NULL,"
                        + "`_op_` STRING NOT NULL,"
                        + "INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("uid", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("uid")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.sequence"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isOne();
        Assertions.assertThat(schemaChangeManager.createdTableSchema.getExtraSchemaElements())
                .containsExactly(
                        "`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)",
                        "`_db_` STRING NOT NULL",
                        "`_tb_` STRING NOT NULL",
                        "`_op_` STRING NOT NULL",
                        "INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED");
        Assertions.assertThat(applier.getCachedSchema(TableId.parse("test.sequence")))
                .isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventSkipsDefaultValuesWhenDisabled() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("schema.change.default_value", "false");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), null, "1")
                        .physicalColumn("name", DataTypes.VARCHAR(17), null, "doris")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.defaults"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isOne();
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("id")
                                .getDefaultValue())
                .isNull();
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("name")
                                .getDefaultValue())
                .isNull();
        Assertions.assertThat(applier.getCachedSchema(TableId.parse("test.defaults")))
                .isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventSyncsNotNullClausesByDefault() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("name")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.not_nulls"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isOne();
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("id")
                                .getTypeString())
                .isEqualTo("INT NOT NULL");
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("name")
                                .getTypeString())
                .isEqualTo("VARCHAR(65533) NOT NULL");
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("description")
                                .getTypeString())
                .isEqualTo("STRING");
    }

    @Test
    public void testCreateTableEventSkipsNotNullClausesWhenDisabled() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("schema.change.null_enable", "false");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(17).notNull())
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.not_nulls"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isOne();
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("id")
                                .getTypeString())
                .isEqualTo("INT");
        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("name")
                                .getTypeString())
                .isEqualTo("VARCHAR(51)");
        Assertions.assertThat(
                        applier.getCachedSchema(TableId.parse("test.not_nulls")).getColumn("id"))
                .hasValueSatisfying(
                        column -> Assertions.assertThat(column.getType().isNullable()).isFalse());
    }

    @Test
    public void testCreateTableEventDoesNotDuplicateAutoPartitionNotNullClause() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put(
                "table.create.auto-partition.properties.default-partition-key", "create_time");
        configMap.put("table.create.auto-partition.properties.default-partition-unit", "year");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("create_time", DataTypes.TIMESTAMP(3).notNull())
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.not_nulls"), targetSchema));

        Assertions.assertThat(
                        schemaChangeManager
                                .createdTableSchema
                                .getFields()
                                .get("create_time")
                                .getTypeString())
                .isEqualTo("DATETIMEV2(3)");
        Assertions.assertThat(
                        DorisSchemaFactory.generateCreateTableDDL(
                                schemaChangeManager.createdTableSchema))
                .contains("`create_time` DATETIMEV2(3) NOT NULL")
                .doesNotContain("NOT NULL NOT NULL");
    }

    @Test
    public void testCreateTableEventFiltersExtraSchemaDuplicateColumns() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put(
                "table.create.extra-schema",
                "`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),"
                        + "`_db_` STRING NOT NULL,"
                        + "INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("uid", DataTypes.BIGINT().notNull())
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP())
                        .primaryKey("uid")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.sequence"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createdTableSchema.getExtraSchemaElements())
                .containsExactly(
                        "`_db_` STRING NOT NULL",
                        "INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED");
    }

    @Test
    public void testCreateTableEventIgnoresExtraSchemaForExistingTable() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("table.create.extra-schema", "`_db_` STRING NOT NULL");
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(configMap),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("uid", "BIGINT"), dorisField("name", "STRING")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("uid", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("uid")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.sequence"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.createdTableSchema).isNull();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
    }

    @Test
    public void testCreateTableEventFailsFastForInvalidExtraSchema() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("table.create.extra-schema", "`_db_` STRING NOT NULL;");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("uid", DataTypes.BIGINT().notNull())
                        .primaryKey("uid")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                TableId.parse("test.sequence"), targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Invalid table.create.extra-schema");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventAcceptsExistingPhysicalColumnAlreadyPresent() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("DEPLOY_MODE", "INT"),
                                        dorisField("project_id", "BIGINT"),
                                        dorisVarcharField("job_name", 51),
                                        dorisDateTimeField("CONFLUENT__LAST_UPDATED", 3)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.VARCHAR(17))
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingVarcharWithoutPrecision() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisField("name", "VARCHAR")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(64))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingTextForStringColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisField("payload", "TEXT")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("payload", DataTypes.STRING())
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingTypesWithSufficientCapacity() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"),
                                        dorisDecimalField("amount", 20, 8),
                                        dorisDateTimeField("created_at", 6)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("amount", DataTypes.DECIMAL(17, 7))
                        .physicalColumn("created_at", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingTinyintWithPrecisionForBooleanColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisField("deleted", "TINYINT")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("deleted", DataTypes.BOOLEAN())
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingDatetimeZeroScaleForTimestampZero() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"),
                                        dorisDateTimeField("created_at", 0)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP_LTZ(0))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsRealDorisSchemaPayloadTypeAliases() throws Exception {
        org.apache.doris.flink.rest.models.Schema existingDorisSchema =
                RestService.parseSchema(
                        "{"
                                + "\"status\":200,"
                                + "\"properties\":["
                                + "{\"name\":\"id\",\"type\":\"INT\",\"precision\":0,\"scale\":0},"
                                + "{\"name\":\"name\",\"type\":\"VARCHAR\",\"precision\":0,\"scale\":0},"
                                + "{\"name\":\"payload\",\"type\":\"TEXT\",\"precision\":0,\"scale\":0},"
                                + "{\"name\":\"deleted\",\"type\":\"TINYINT\",\"precision\":0,\"scale\":0},"
                                + "{\"name\":\"amount\",\"type\":\"DECIMALV3\",\"precision\":20,\"scale\":8},"
                                + "{\"name\":\"created_at\",\"type\":\"DATETIMEV2\",\"precision\":0,\"scale\":6}"
                                + "]"
                                + "}",
                        LoggerFactory.getLogger(DorisMetadataApplierTest.class));
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> existingDorisSchema);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(64))
                        .physicalColumn("payload", DataTypes.STRING())
                        .physicalColumn("deleted", DataTypes.BOOLEAN())
                        .physicalColumn("amount", DataTypes.DECIMAL(17, 7))
                        .physicalColumn("created_at", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventFailsWhenExistingTableSchemaLookupReturnsNull() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> null,
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.TABLE_EXISTS);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Failed to resolve existing Doris schema")
                .hasRootCauseMessage(
                        "Doris table streampark.t_flink_app exists but schema lookup returned null");

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventCreatesTableWhenDatabaseIsAbsent() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.seedDorisSchema(TABLE_ID, createDorisSchema());
        DorisMetadataApplier applier =
                new TestDorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> schemaChangeManager.getDorisSchema(tableId),
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.DATABASE_ABSENT);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isEqualTo(1);
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name");
    }

    @Test
    public void testCreateTableEventCreatesTableWhenTableIsAbsent() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.seedDorisSchema(TABLE_ID, createDorisSchema());
        DorisMetadataApplier applier =
                new TestDorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> schemaChangeManager.getDorisSchema(tableId),
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.TABLE_ABSENT);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isEqualTo(1);
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name");
    }

    @Test
    public void testCreateTableEventReconcilesWhenTableExistenceCheckerFindsTable() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisVarcharField("name", 51)),
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.TABLE_EXISTS);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventFailsWhenTableExistenceCheckFails() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> {
                            throw new AssertionError("schema fetch should not be called");
                        },
                        (dorisOptions, tableId) -> {
                            throw new IllegalStateException("existence query permission denied");
                        });

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasRootCauseMessage("existence query permission denied");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventDoesNotInferAbsentTableFromSchemaLookupTableNotFoundMessage() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> {
                            throw new RuntimeException(
                                    "can not parse response schema \"errCode = 7, "
                                            + "detailMessage = table not found, "
                                            + "tableName=daily_stock\"");
                        },
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.TABLE_EXISTS);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Failed to resolve existing Doris schema")
                .hasRootCauseMessage(
                        "can not parse response schema \"errCode = 7, "
                                + "detailMessage = table not found, tableName=daily_stock\"");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventDoesNotInferAbsentTableFromGenericHttp404Message() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> {
                            throw new RuntimeException(
                                    "Failed to parse response, status: 404, reason: Not Found");
                        },
                        (dorisOptions, tableId) ->
                                DorisTableExistenceChecker.Existence.TABLE_EXISTS);

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Failed to resolve existing Doris schema")
                .hasRootCauseMessage("Failed to parse response, status: 404, reason: Not Found");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventFailsWhenExistingSchemaLookupThrowsUnexpectedException() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> {
                            throw new RuntimeException("connection refused");
                        });

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(org.apache.flink.cdc.common.exceptions.SchemaEvolveException.class)
                .hasMessageContaining("Failed to resolve existing Doris schema")
                .hasRootCauseMessage("connection refused");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
    }

    @Test
    public void testCreateTableEventReconcileKeepsNotNullColumnInCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("DEPLOY_MODE", "INT"),
                                dorisField("project_id", "BIGINT"),
                                dorisVarcharField("job_name", 51)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.VARCHAR(17))
                        .physicalColumn("tracked_flag", DataTypes.INT().notNull())
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnName)
                .isEqualTo("tracked_flag");
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnType)
                .isEqualTo("INT NOT NULL");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingColumnTypeDrift() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("DEPLOY_MODE", "INT"),
                                        dorisField("project_id", "BIGINT"),
                                        dorisVarcharField("job_name", 51),
                                        dorisVarcharField("CONFLUENT__LAST_UPDATED", 64)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.VARCHAR(17))
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingDecimalWithoutPrecisionMetadata() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisField("amount", "DECIMAL")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("amount", DataTypes.DECIMAL(17, 7))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingDatetimeWithoutScaleMetadata() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"),
                                        dorisField("created_at", "DATETIME")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventDoesNotRejectExistingVarcharWithCapacityRisk() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisVarcharField("name", 32)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingCharForVarcharColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisCharField("name", 51)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingTinyintForBooleanColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"), dorisTinyintField("deleted", 1)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("deleted", DataTypes.BOOLEAN())
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventDoesNotRejectExistingDecimalWithCapacityRisk() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"),
                                        dorisDecimalField("amount", 16, 7)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("amount", DataTypes.DECIMAL(17, 7))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventDoesNotRejectExistingDatetimeWithCapacityRisk() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "INT"),
                                        dorisDateTimeField("created_at", 3)));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP_LTZ(6))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventAcceptsExistingDatetimeZeroScaleForTimestampThree() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema(
                                        dorisField("id", "BIGINT"),
                                        dorisVarcharField("name", 192),
                                        dorisDateTimeField("create_time", 0),
                                        dorisField("_db_", "STRING"),
                                        dorisField("_tb_", "STRING"),
                                        dorisField("_op_", "STRING")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("create_time", DataTypes.TIMESTAMP(3))
                        .physicalColumn("_db_", DataTypes.STRING().notNull())
                        .physicalColumn("_tb_", DataTypes.STRING().notNull())
                        .physicalColumn("_op_", DataTypes.STRING().notNull())
                        .primaryKey("id")
                        .build();

        applier.applySchemaChange(
                new CreateTableEvent(TableId.parse("test.student"), targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TableId.parse("test.student")))
                .isEqualTo(targetSchema);
    }

    @Test
    public void testCreateTableEventFailsAndDoesNotCacheSchemaWhenCreateTableReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.createTableResult = false;
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("create table");
        Assertions.assertThat(schemaChangeManager.createTableInvocations).isEqualTo(1);
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testCreateTableEventFailsAndDoesNotCacheSchemaWhenReconcileAddColumnReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.addColumnResult = false;
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> createDorisSchema(dorisField("id", "INT")));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, targetSchema)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("add missing column name");
        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventFailsAndKeepsPreviousCacheWhenAddColumnReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        schemaChangeManager.addColumnResult = false;

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AddColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "name",
                                                                        DataTypes.VARCHAR(17)))))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("add column name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
    }

    @Test
    public void testAddColumnEventAppliesDorisDdlWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn(
                                                "age", DataTypes.BIGINT().notNull(), "age comment"),
                                        "name"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnName).isEqualTo("age");
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).referenceColumn)
                .isEqualTo("name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventKeepsCdcNullabilityInCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "tracked_flag", DataTypes.INT().notNull())))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("tracked_flag"))
                .hasValueSatisfying(
                        column -> Assertions.assertThat(column.getType().isNullable()).isFalse());
    }

    @Test
    public void testAddColumnEventAppliesAfterPositionToDorisDdl() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                        .physicalColumn("create_time", DataTypes.TIMESTAMP(3))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn("age", DataTypes.INT(), null, "18"),
                                        "name"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.columnName).isEqualTo("age");
        Assertions.assertThat(addedColumn.columnType).isEqualTo("INT");
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "name", "age", "create_time");
    }

    @Test
    public void testAddColumnAfterPositionResolvesPhysicalNameWhenOrderCacheInvalidated()
            throws Exception {
        // When a prior add-column refresh times out (Doris FE has not reflected the new column
        // within the refresh budget), the physical column-order cache is invalidated and
        // currentColumnNames becomes null for the following column. The AFTER position must still
        // resolve the Doris physical reference name instead of leaking the raw CDC logical name.
        // Doris physical columns are [ID, JOB]; the event references the existing column by its
        // logical name "job", which must be translated to the physical name "JOB".
        // The fixed fetcher never reflects the added columns, simulating a slow Doris FE so the
        // first add invalidates the order cache for the second.
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.seedDorisSchema(
                TABLE_ID, createDorisSchema(dorisField("ID", "INT"), dorisField("JOB", "STRING")));
        final org.apache.doris.flink.rest.models.Schema fixedDorisSchema =
                createDorisSchema(dorisField("ID", "INT"), dorisField("JOB", "STRING"));
        DorisMetadataApplier applier =
                new TestDorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) -> fixedDorisSchema);

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("c1", DataTypes.INT(), null, null)),
                                AddColumnEvent.after(
                                        Column.physicalColumn("c2", DataTypes.INT(), null, null),
                                        "job"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(2);
        AddedColumn secondAdd = schemaChangeManager.addedColumns.get(1);
        Assertions.assertThat(secondAdd.columnName).isEqualTo("c2");
        Assertions.assertThat(secondAdd.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(secondAdd.referenceColumn).isEqualTo("JOB");
    }

    @Test
    public void testAddColumnEventSkipsDefaultValueWhenDisabled() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("schema.change.default_value", "false");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Column addedColumn = Column.physicalColumn("age", DataTypes.INT(), null, "18");
        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID, Collections.singletonList(AddColumnEvent.last(addedColumn))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).defaultValue).isNull();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("age"))
                .hasValueSatisfying(
                        column ->
                                Assertions.assertThat(column.getDefaultValueExpression())
                                        .isEqualTo("18"));
    }

    @Test
    public void testAddColumnEventSyncsNotNullClauseByDefault() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Column addedColumn = Column.physicalColumn("age", DataTypes.INT().notNull());
        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID, Collections.singletonList(AddColumnEvent.last(addedColumn))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnType)
                .isEqualTo("INT NOT NULL");
    }

    @Test
    public void testAddColumnEventSkipsNotNullClauseWhenDisabled() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("schema.change.null_enable", "false");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Column addedColumn = Column.physicalColumn("age", DataTypes.INT().notNull());
        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID, Collections.singletonList(AddColumnEvent.last(addedColumn))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).columnType).isEqualTo("INT");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("age"))
                .hasValueSatisfying(
                        column -> Assertions.assertThat(column.getType().isNullable()).isFalse());
    }

    @Test
    public void testAddColumnEventTranslatesBeforePositionToDorisAfterPreviousColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .physicalColumn("score", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("age", DataTypes.INT()), "score"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "name", "age", "score");
    }

    @Test
    public void testAddColumnEventTranslatesBeforeFirstKeyColumnToFirstValueColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("rank", DataTypes.INT()), "id"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("id");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "rank", "name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("rank", "id", "name");
    }

    @Test
    public void testAddColumnEventTranslatesFirstPositionToFirstValueColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .primaryKey("id")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.first(
                                        Column.physicalColumn("rank", DataTypes.INT())))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("id");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "rank", "name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("rank", "id", "name");
    }

    @Test
    public void testAddColumnEventTranslatesBeforePositionWithMissingSchemaCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("age", DataTypes.INT()), "score"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(addedColumn.referenceColumn).isEqualTo("name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventTranslatesBeforeFirstColumnWithMissingSchemaCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("rank", DataTypes.INT()), "id"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        AddedColumn addedColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(addedColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.FIRST);
        Assertions.assertThat(addedColumn.referenceColumn).isNull();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventUsesUpdatedColumnOrderWithMissingSchemaCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT")));

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                AddColumnEvent.before(Column.physicalColumn("age", DataTypes.INT()), "score"));
        addedColumns.add(
                AddColumnEvent.before(Column.physicalColumn("level", DataTypes.INT()), "score"));
        applier.applySchemaChange(new AddColumnEvent(TABLE_ID, addedColumns));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(2);
        AddedColumn ageColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(ageColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(ageColumn.referenceColumn).isEqualTo("name");
        AddedColumn levelColumn = schemaChangeManager.addedColumns.get(1);
        Assertions.assertThat(levelColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(levelColumn.referenceColumn).isEqualTo("age");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventRefreshesActualDorisOrderAfterPositionFallback() {
        RecordingSchemaChangeManager schemaChangeManager =
                new PositionFallbackRecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("age", DataTypes.INT()), "score"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).referenceColumn)
                .isEqualTo("name");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name", "score", "age");

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("level", DataTypes.INT()), "age"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(2);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(1).referenceColumn)
                .isEqualTo("score");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name", "score", "level", "age");
    }

    @Test
    public void testAddColumnEventRefreshesActualDorisOrderWhenColumnAlreadyExists() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT"),
                                dorisField("age", "INT")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("age", DataTypes.INT()), "score"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name", "score", "age");

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.before(
                                        Column.physicalColumn("level", DataTypes.INT()), "age"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(2);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(1).referenceColumn)
                .isEqualTo("score");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name", "score", "level", "age");
    }

    @Test
    public void testAddColumnEventInvalidatesPhysicalOrderCacheWhenDorisSchemaDoesNotCatchUp() {
        RecordingSchemaChangeManager schemaChangeManager =
                new StaleAddColumnRecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("id", "INT"), dorisField("name", "STRING")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("age", DataTypes.INT())))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID)).isNull();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventUsesFirstPositionWhenUpdatingColumnOrderWithMissingSchemaCache() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(
                                dorisField("id", "INT"),
                                dorisField("name", "STRING"),
                                dorisField("score", "INT")));

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(AddColumnEvent.first(Column.physicalColumn("rank", DataTypes.INT())));
        addedColumns.add(
                AddColumnEvent.before(Column.physicalColumn("level", DataTypes.INT()), "id"));
        applier.applySchemaChange(new AddColumnEvent(TABLE_ID, addedColumns));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(2);
        AddedColumn rankColumn = schemaChangeManager.addedColumns.get(0);
        Assertions.assertThat(rankColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.FIRST);
        Assertions.assertThat(rankColumn.referenceColumn).isNull();
        AddedColumn levelColumn = schemaChangeManager.addedColumns.get(1);
        Assertions.assertThat(levelColumn.positionType)
                .isEqualTo(AddColumnPosition.PositionType.AFTER);
        Assertions.assertThat(levelColumn.referenceColumn).isEqualTo("rank");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventFailsWhenBeforeReferenceIsMissingFromFetchedDorisSchema() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("id", "INT"), dorisField("name", "STRING")));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AddColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonList(
                                                        AddColumnEvent.before(
                                                                Column.physicalColumn(
                                                                        "age", DataTypes.INT()),
                                                                "score")))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasRootCauseMessage(
                        "Cannot find reference column score while translating CDC BEFORE column position to Doris ADD COLUMN position.");
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAddColumnEventInvalidatesStaleSchemaCacheWhenLocalUpdateFails() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema staleSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, staleSchema));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn("age", DataTypes.BIGINT()),
                                        "name"))));

        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAlterColumnTypeEventFailsAndKeepsPreviousCacheWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        schemaChangeManager.modifyColumnDataTypeResult = false;

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("name", DataTypes.VARCHAR(64));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AlterColumnTypeEvent(TABLE_ID, typeMapping)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("alter column type name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
    }

    @Test
    public void testAlterColumnTypeEventDoesNotFailWhenRestSchemaTypeLags() {
        RecordingSchemaChangeManager schemaChangeManager =
                new StaleModifyColumnRecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("name", DataTypes.VARCHAR(64));

        applier.applySchemaChange(new AlterColumnTypeEvent(TABLE_ID, typeMapping));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "name");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column ->
                                Assertions.assertThat(column.getType())
                                        .isEqualTo(DataTypes.VARCHAR(64)));
    }

    @Test
    public void testAlterColumnTypeEventAppliesDorisDdlWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("zyb_ticket_code", "VARCHAR")));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("zyb_ticket_code", DataTypes.VARCHAR(64));

        applier.applySchemaChange(new AlterColumnTypeEvent(TABLE_ID, typeMapping));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnName)
                .isEqualTo("zyb_ticket_code");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnType)
                .isEqualTo("VARCHAR(192)");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testAlterColumnTypeEventResolvesDorisPhysicalNameWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("ID", "INT"), dorisField("JOB", "STRING")));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("job", DataTypes.VARCHAR(64)),
                        Collections.emptyMap(),
                        Collections.singletonMap("job", "new job")));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnName)
                .isEqualTo("JOB");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).comment)
                .isEqualTo("new job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("ID", "JOB");
    }

    @Test
    public void testAlterColumnTypeEventSyncsNotNullClauseByDefault() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("name", DataTypes.VARCHAR(64).notNull());
        applier.applySchemaChange(new AlterColumnTypeEvent(TABLE_ID, typeMapping));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnName)
                .isEqualTo("name");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnType)
                .isEqualTo("VARCHAR(192) NOT NULL");
    }

    @Test
    public void testAlterColumnTypeEventSkipsNotNullClauseWhenDisabled() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("schema.change.null_enable", "false");
        DorisMetadataApplier applier =
                createApplierForAbsentTable(schemaChangeManager, Configuration.fromMap(configMap));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("name", DataTypes.VARCHAR(64).notNull());
        applier.applySchemaChange(new AlterColumnTypeEvent(TABLE_ID, typeMapping));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnType)
                .isEqualTo("VARCHAR(192)");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column -> Assertions.assertThat(column.getType().isNullable()).isFalse());
    }

    @Test
    public void testAlterColumnTypeEventCombinesTypeAndCommentDdl() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17), "old comment", "unknown")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("name", DataTypes.VARCHAR(64)),
                        Collections.emptyMap(),
                        Collections.singletonMap("name", "new comment")));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnName)
                .isEqualTo("name");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnType)
                .isEqualTo("VARCHAR(192)");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).defaultValue)
                .isEqualTo("unknown");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).comment)
                .isEqualTo("new comment");
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column -> {
                            Assertions.assertThat(column.getType())
                                    .isEqualTo(DataTypes.VARCHAR(64));
                            Assertions.assertThat(column.getComment()).isEqualTo("new comment");
                        });
    }

    @Test
    public void testAlterColumnTypeEventClearsCommentWhenTypeChangesWithNullComment() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17), "old comment")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("name", DataTypes.VARCHAR(64)),
                        Collections.emptyMap(),
                        Collections.singletonMap("name", null)));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).comment)
                .isEqualTo("old comment");
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments)
                .containsExactly(Collections.singletonMap("name", ""));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column -> {
                            Assertions.assertThat(column.getType())
                                    .isEqualTo(DataTypes.VARCHAR(64));
                            Assertions.assertThat(column.getComment()).isNull();
                        });
    }

    @Test
    public void testAlterColumnTypeEventClearsCommentWhenTypeChangesWithEmptyComment() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(17), "old comment")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("name", DataTypes.VARCHAR(64)),
                        Collections.emptyMap(),
                        Collections.singletonMap("name", "")));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).comment)
                .isEqualTo("old comment");
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments)
                .containsExactly(Collections.singletonMap("name", ""));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column -> {
                            Assertions.assertThat(column.getType())
                                    .isEqualTo(DataTypes.VARCHAR(64));
                            Assertions.assertThat(column.getComment()).isEmpty();
                        });
    }

    @Test
    public void testAlterColumnTypeEventClearsCommentWhenTypeIsUnchangedWithNullComment() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(64), "old comment")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("name", DataTypes.VARCHAR(64)),
                        Collections.emptyMap(),
                        Collections.singletonMap("name", null)));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).isEmpty();
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments)
                .containsExactly(Collections.singletonMap("name", ""));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("name"))
                .hasValueSatisfying(
                        column -> {
                            Assertions.assertThat(column.getType())
                                    .isEqualTo(DataTypes.VARCHAR(64));
                            Assertions.assertThat(column.getComment()).isNull();
                        });
    }

    @Test
    public void testAlterColumnTypeEventResolvesColumnNameAndAppliesCommentOnlyChange() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64), "old job comment")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.singletonMap("JOB", "new job comment")));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).isEmpty();
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments)
                .containsExactly(Collections.singletonMap("job", "new job comment"));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("job"))
                .hasValueSatisfying(
                        column ->
                                Assertions.assertThat(column.getComment())
                                        .isEqualTo("new job comment"));
    }

    @Test
    public void testDropColumnEventResolvesColumnNameCaseInsensitively() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .physicalColumn("age", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(new DropColumnEvent(TABLE_ID, Collections.singletonList("JOB")));

        Assertions.assertThat(schemaChangeManager.droppedColumns).containsExactly("job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "age");
    }

    @Test
    public void testRenameColumnEventResolvesOldColumnNameCaseInsensitively() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("JOB", "job_name")));

        Assertions.assertThat(schemaChangeManager.renamedColumns)
                .containsExactly(Collections.singletonMap("job", "job_name"));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "job_name");
    }

    @Test
    public void testRenameColumnEventSkipsDorisDdlForCaseOnlyRename() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("JOB", DataTypes.VARCHAR(64))
                        .physicalColumn("age", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("JOB", "job")));

        Assertions.assertThat(schemaChangeManager.renamedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "job", "age");
    }

    @Test
    public void testAddColumnAfterCaseOnlyRenameKeepsDorisPhysicalReferenceColumn() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("JOB", DataTypes.VARCHAR(64))
                        .physicalColumn("age", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("JOB", "job")));

        applier.applySchemaChange(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn("score", DataTypes.INT()), "job"))));

        Assertions.assertThat(schemaChangeManager.renamedColumns).isEmpty();
        Assertions.assertThat(schemaChangeManager.addedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.addedColumns.get(0).referenceColumn)
                .isEqualTo("JOB");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "job", "score", "age");
    }

    @Test
    public void testAlterAfterCaseOnlyRenameKeepsDorisPhysicalColumnName() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("JOB", DataTypes.VARCHAR(64), "old job")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("JOB", "job")));

        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("job", DataTypes.VARCHAR(128)),
                        Collections.emptyMap(),
                        Collections.singletonMap("job", "new job")));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).hasSize(1);
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).columnName)
                .isEqualTo("JOB");
        Assertions.assertThat(schemaChangeManager.modifiedColumns.get(0).comment)
                .isEqualTo("new job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("job"))
                .hasValueSatisfying(
                        column -> {
                            Assertions.assertThat(column.getType())
                                    .isEqualTo(DataTypes.VARCHAR(128));
                            Assertions.assertThat(column.getComment()).isEqualTo("new job");
                        });
    }

    @Test
    public void testDropAfterCaseOnlyRenameKeepsDorisPhysicalColumnName() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("JOB", DataTypes.VARCHAR(64))
                        .physicalColumn("age", DataTypes.INT())
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("JOB", "job")));

        applier.applySchemaChange(new DropColumnEvent(TABLE_ID, Collections.singletonList("job")));

        Assertions.assertThat(schemaChangeManager.droppedColumns).containsExactly("JOB");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumnNames())
                .containsExactly("id", "age");
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("id", "age");
    }

    @Test
    public void testCommentRemovalResolvesColumnNameCaseInsensitively() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64), "old job comment")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Map<String, String> comments = new HashMap<>();
        comments.put("JOB", null);
        applier.applySchemaChange(
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.emptyMap(), Collections.emptyMap(), comments));

        Assertions.assertThat(schemaChangeManager.modifiedColumns).isEmpty();
        Assertions.assertThat(schemaChangeManager.modifiedColumnComments)
                .containsExactly(Collections.singletonMap("job", ""));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID).getColumn("job"))
                .hasValueSatisfying(column -> Assertions.assertThat(column.getComment()).isNull());
    }

    @Test
    public void testDropColumnEventFailsAndKeepsPreviousCacheWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        schemaChangeManager.dropColumnResult = false;

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new DropColumnEvent(
                                                TABLE_ID, Collections.singletonList("JOB"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("drop column job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
    }

    @Test
    public void testDropFailsIfDorisSchemaStale() {
        RecordingSchemaChangeManager schemaChangeManager =
                new StaleDropColumnRecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new DropColumnEvent(
                                                TABLE_ID, Collections.singletonList("job"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasStackTraceContaining("Failed to refresh Doris physical column-order cache")
                .hasStackTraceContaining("excludes columns [job]");
        Assertions.assertThat(schemaChangeManager.droppedColumns).containsExactly("job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID)).isNull();
    }

    @Test
    public void testRenameColumnEventFailsAndKeepsPreviousCacheWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        schemaChangeManager.renameColumnResult = false;

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonMap("JOB", "job_name"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("rename column job");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
    }

    @Test
    public void testRenameFailsIfDorisSchemaStale() {
        RecordingSchemaChangeManager schemaChangeManager =
                new StaleRenameColumnRecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("job", DataTypes.VARCHAR(64))
                        .build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonMap("job", "job_name"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasStackTraceContaining("Failed to refresh Doris physical column-order cache")
                .hasStackTraceContaining("contains columns [job_name]");
        Assertions.assertThat(schemaChangeManager.renamedColumns)
                .containsExactly(Collections.singletonMap("job", "job_name"));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID)).isNull();
    }

    @Test
    public void testDropColumnEventAppliesDorisDdlWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("legacy_column", "STRING")));

        applier.applySchemaChange(
                new DropColumnEvent(TABLE_ID, Collections.singletonList("legacy_column")));

        Assertions.assertThat(schemaChangeManager.droppedColumns).containsExactly("legacy_column");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testDropColumnEventResolvesDorisPhysicalNameWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("ID", "INT"), dorisField("JOB", "STRING")));

        applier.applySchemaChange(new DropColumnEvent(TABLE_ID, Collections.singletonList("job")));

        Assertions.assertThat(schemaChangeManager.droppedColumns).containsExactly("JOB");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID)).containsExactly("ID");
    }

    @Test
    public void testDropColumnEventIsIdempotentWhenDorisColumnIsAlreadyAbsent() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager, createDorisSchema(dorisField("ID", "INT")));

        applier.applySchemaChange(new DropColumnEvent(TABLE_ID, Collections.singletonList("job")));

        Assertions.assertThat(schemaChangeManager.droppedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID)).containsExactly("ID");
    }

    @Test
    public void testRenameColumnEventAppliesDorisDdlWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager, createDorisSchema(dorisField("old_column", "STRING")));

        applier.applySchemaChange(
                new RenameColumnEvent(
                        TABLE_ID, Collections.singletonMap("old_column", "new_column")));

        Assertions.assertThat(schemaChangeManager.renamedColumns)
                .containsExactly(Collections.singletonMap("old_column", "new_column"));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
    }

    @Test
    public void testRenameColumnEventResolvesDorisPhysicalNameWhenSchemaCacheIsMissing() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager,
                        createDorisSchema(dorisField("ID", "INT"), dorisField("JOB", "STRING")));

        applier.applySchemaChange(
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("job", "job_name")));

        Assertions.assertThat(schemaChangeManager.renamedColumns)
                .containsExactly(Collections.singletonMap("JOB", "job_name"));
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isNull();
        Assertions.assertThat(applier.getCachedDorisColumnOrder(TABLE_ID))
                .containsExactly("ID", "job_name");
    }

    @Test
    public void testRenameColumnEventFailsWhenPhysicalColumnNameCannotBeResolved() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForExistingDorisSchema(
                        schemaChangeManager, createDorisSchema(dorisField("ID", "INT")));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonMap("job", "job_name"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasStackTraceContaining("Failed to refresh Doris physical column-order cache");
        Assertions.assertThat(schemaChangeManager.renamedColumns).isEmpty();
    }

    @Test
    public void testTruncateTableEventFailsWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.truncateTableResult = false;
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Assertions.assertThatThrownBy(
                        () -> applier.applySchemaChange(new TruncateTableEvent(TABLE_ID)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("truncate table");
    }

    @Test
    public void testDropTableEventFailsAndKeepsCacheWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Schema initialSchema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema));
        schemaChangeManager.dropTableResult = false;

        Assertions.assertThatThrownBy(() -> applier.applySchemaChange(new DropTableEvent(TABLE_ID)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("drop table");
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(initialSchema);
    }

    @Test
    public void testAlterTableCommentEventFailsWhenDorisReturnsFalse() {
        RecordingSchemaChangeManager schemaChangeManager = new RecordingSchemaChangeManager();
        schemaChangeManager.alterTableCommentResult = false;
        DorisMetadataApplier applier =
                createApplierForAbsentTable(
                        schemaChangeManager, Configuration.fromMap(Collections.emptyMap()));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AlterTableCommentEvent(TABLE_ID, "new comment")))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("Doris schema change returned false")
                .hasMessageContaining("alter table comment");
    }

    private static DorisOptions createDorisOptions() {
        return DorisOptions.builder()
                .setFenodes("127.0.0.1:8030")
                .setUsername("root")
                .setPassword("")
                .build();
    }

    private static DorisMetadataApplier createApplierForAbsentTable(
            RecordingSchemaChangeManager schemaChangeManager, Configuration config) {
        schemaChangeManager.seedDorisSchema(TABLE_ID, createDorisSchema());
        return new TestDorisMetadataApplier(
                createDorisOptions(),
                config,
                schemaChangeManager,
                (dorisOptions, tableId) -> schemaChangeManager.getDorisSchema(tableId),
                (dorisOptions, tableId) -> DorisTableExistenceChecker.Existence.TABLE_ABSENT);
    }

    private static DorisMetadataApplier createApplierForExistingDorisSchema(
            RecordingSchemaChangeManager schemaChangeManager,
            org.apache.doris.flink.rest.models.Schema dorisSchema) {
        schemaChangeManager.seedDorisSchema(TABLE_ID, dorisSchema);
        return new TestDorisMetadataApplier(
                createDorisOptions(),
                Configuration.fromMap(Collections.emptyMap()),
                schemaChangeManager,
                (dorisOptions, tableId) -> schemaChangeManager.getDorisSchema(tableId));
    }

    private static class TestDorisMetadataApplier extends DorisMetadataApplier {
        private TestDorisMetadataApplier(
                DorisOptions dorisOptions,
                Configuration config,
                DorisSchemaChangeManager schemaChangeManager,
                DorisSchemaFetcher dorisSchemaFetcher) {
            super(dorisOptions, config, schemaChangeManager, dorisSchemaFetcher);
        }

        private TestDorisMetadataApplier(
                DorisOptions dorisOptions,
                Configuration config,
                DorisSchemaChangeManager schemaChangeManager,
                DorisSchemaFetcher dorisSchemaFetcher,
                DorisTableExistenceChecker tableExistenceChecker) {
            super(
                    dorisOptions,
                    config,
                    schemaChangeManager,
                    dorisSchemaFetcher,
                    tableExistenceChecker);
        }

        @Override
        protected void sleepBeforeDorisRetry() {
            // The recording schema manager applies schema changes synchronously.
        }

        @Override
        protected long dorisRefreshMaxWaitMillis() {
            return 0L;
        }

        @Override
        protected long dorisRefreshRetryMillis() {
            return 1L;
        }
    }

    private static org.apache.doris.flink.rest.models.Schema createDorisSchema(
            org.apache.doris.flink.rest.models.Field... fields) {
        org.apache.doris.flink.rest.models.Schema dorisSchema =
                new org.apache.doris.flink.rest.models.Schema(fields.length);
        for (org.apache.doris.flink.rest.models.Field field : fields) {
            dorisSchema.put(field);
        }
        return dorisSchema;
    }

    private static org.apache.doris.flink.rest.models.Field dorisField(String name, String type) {
        return new org.apache.doris.flink.rest.models.Field(name, type, null, 0, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisVarcharField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "VARCHAR", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisCharField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(name, "CHAR", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisTinyintField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "TINYINT", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisDecimalField(
            String name, int precision, int scale) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "DECIMAL", null, precision, scale, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisDateTimeField(
            String name, int scale) {
        return new org.apache.doris.flink.rest.models.Field(name, "DATETIME", null, 0, scale, null);
    }

    private static class RecordingSchemaChangeManager extends DorisSchemaChangeManager {
        private int createTableInvocations;
        private org.apache.doris.flink.catalog.doris.TableSchema createdTableSchema;
        private boolean createTableResult = true;
        private boolean addColumnResult = true;
        private boolean modifyColumnDataTypeResult = true;
        private boolean dropColumnResult = true;
        private boolean renameColumnResult = true;
        private boolean truncateTableResult = true;
        private boolean dropTableResult = true;
        private boolean alterTableCommentResult = true;
        private boolean modifyColumnCommentResult = true;
        private final List<AddedColumn> addedColumns = new ArrayList<>();
        private final List<ModifiedColumn> modifiedColumns = new ArrayList<>();
        private final List<Map<String, String>> modifiedColumnComments = new ArrayList<>();
        private final List<String> droppedColumns = new ArrayList<>();
        private final List<Map<String, String>> renamedColumns = new ArrayList<>();
        private final Map<String, List<org.apache.doris.flink.rest.models.Field>> physicalSchemas =
                new HashMap<>();

        private RecordingSchemaChangeManager() {
            super(createDorisOptions(), null);
        }

        private void seedDorisSchema(
                TableId tableId, org.apache.doris.flink.rest.models.Schema dorisSchema) {
            physicalSchemas.put(tableKey(tableId), copyFields(dorisSchema.getProperties()));
        }

        private org.apache.doris.flink.rest.models.Schema getDorisSchema(TableId tableId) {
            List<org.apache.doris.flink.rest.models.Field> fields =
                    physicalSchemas.get(tableKey(tableId));
            if (fields == null) {
                throw new IllegalStateException("No Doris schema is recorded for " + tableId);
            }
            return createDorisSchema(
                    copyFields(fields).toArray(new org.apache.doris.flink.rest.models.Field[0]));
        }

        @Override
        public boolean createTable(org.apache.doris.flink.catalog.doris.TableSchema tableSchema) {
            createTableInvocations++;
            createdTableSchema = tableSchema;
            if (createTableResult) {
                List<org.apache.doris.flink.rest.models.Field> fields = new ArrayList<>();
                tableSchema
                        .getFields()
                        .values()
                        .forEach(fieldSchema -> fields.add(dorisField(fieldSchema)));
                physicalSchemas.put(
                        tableKey(tableSchema.getDatabase(), tableSchema.getTable()), fields);
            }
            return createTableResult;
        }

        @Override
        public boolean addColumn(
                String databaseName, String tableName, FieldSchema addFieldSchema) {
            addedColumns.add(new AddedColumn(addFieldSchema, AddColumnPosition.last()));
            if (addColumnResult) {
                applySuccessfulAddColumn(
                        databaseName, tableName, addFieldSchema, AddColumnPosition.last());
            }
            return addColumnResult;
        }

        @Override
        public boolean addColumn(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition position) {
            addedColumns.add(new AddedColumn(addFieldSchema, position));
            if (addColumnResult) {
                applySuccessfulAddColumn(databaseName, tableName, addFieldSchema, position);
            }
            return addColumnResult;
        }

        @Override
        public boolean modifyColumnDataType(
                String databaseName, String tableName, FieldSchema field) {
            modifiedColumns.add(new ModifiedColumn(field));
            if (modifyColumnDataTypeResult) {
                applySuccessfulModifyColumn(databaseName, tableName, field);
            }
            return modifyColumnDataTypeResult;
        }

        protected void applySuccessfulModifyColumn(
                String databaseName, String tableName, FieldSchema field) {
            int index = findPhysicalColumnIndex(databaseName, tableName, field.getName());
            if (index >= 0) {
                physicalSchema(databaseName, tableName).set(index, dorisField(field));
            }
        }

        @Override
        public boolean modifyColumnComment(
                String databaseName, String tableName, String columnName, String newComment) {
            modifiedColumnComments.add(Collections.singletonMap(columnName, newComment));
            if (modifyColumnCommentResult) {
                int index = findPhysicalColumnIndex(databaseName, tableName, columnName);
                if (index >= 0) {
                    physicalSchema(databaseName, tableName).get(index).setComment(newComment);
                }
            }
            return modifyColumnCommentResult;
        }

        @Override
        public boolean dropColumn(String databaseName, String tableName, String columnName) {
            droppedColumns.add(columnName);
            if (dropColumnResult) {
                applySuccessfulDropColumn(databaseName, tableName, columnName);
            }
            return dropColumnResult;
        }

        protected void applySuccessfulDropColumn(
                String databaseName, String tableName, String columnName) {
            int index = findPhysicalColumnIndex(databaseName, tableName, columnName);
            if (index >= 0) {
                physicalSchema(databaseName, tableName).remove(index);
            }
        }

        @Override
        public boolean renameColumn(
                String databaseName, String tableName, String oldColumnName, String newColumnName) {
            renamedColumns.add(Collections.singletonMap(oldColumnName, newColumnName));
            if (renameColumnResult) {
                applySuccessfulRenameColumn(databaseName, tableName, oldColumnName, newColumnName);
            }
            return renameColumnResult;
        }

        protected void applySuccessfulRenameColumn(
                String databaseName, String tableName, String oldColumnName, String newColumnName) {
            int index = findPhysicalColumnIndex(databaseName, tableName, oldColumnName);
            if (index >= 0) {
                physicalSchema(databaseName, tableName).get(index).setName(newColumnName);
            }
        }

        @Override
        public boolean truncateTable(String databaseName, String tableName) {
            return truncateTableResult;
        }

        @Override
        public boolean dropTable(String databaseName, String tableName) {
            if (dropTableResult) {
                physicalSchemas.remove(tableKey(databaseName, tableName));
            }
            return dropTableResult;
        }

        @Override
        public boolean alterTableComment(String databaseName, String tableName, String comment) {
            return alterTableCommentResult;
        }

        protected void applySuccessfulAddColumn(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition position) {
            List<org.apache.doris.flink.rest.models.Field> fields =
                    physicalSchema(databaseName, tableName);
            if (findPhysicalColumnIndex(fields, addFieldSchema.getName()) >= 0) {
                return;
            }
            int insertIndex = fields.size();
            if (AddColumnPosition.PositionType.FIRST.equals(position.getPositionType())) {
                insertIndex = 0;
            } else if (AddColumnPosition.PositionType.AFTER.equals(position.getPositionType())) {
                int referenceIndex = findPhysicalColumnIndex(fields, position.getReferenceColumn());
                insertIndex = referenceIndex < 0 ? fields.size() : referenceIndex + 1;
            }
            fields.add(insertIndex, dorisField(addFieldSchema));
        }

        private List<org.apache.doris.flink.rest.models.Field> physicalSchema(
                String databaseName, String tableName) {
            return physicalSchemas.computeIfAbsent(
                    tableKey(databaseName, tableName), ignored -> new ArrayList<>());
        }

        private int findPhysicalColumnIndex(
                String databaseName, String tableName, String columnName) {
            return findPhysicalColumnIndex(physicalSchema(databaseName, tableName), columnName);
        }

        private int findPhysicalColumnIndex(
                List<org.apache.doris.flink.rest.models.Field> fields, String columnName) {
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equals(columnName)) {
                    return i;
                }
            }
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equalsIgnoreCase(columnName)) {
                    return i;
                }
            }
            return -1;
        }

        private String tableKey(TableId tableId) {
            return tableKey(tableId.getSchemaName(), tableId.getTableName());
        }

        private String tableKey(String databaseName, String tableName) {
            return databaseName + "." + tableName;
        }

        private static List<org.apache.doris.flink.rest.models.Field> copyFields(
                List<org.apache.doris.flink.rest.models.Field> fields) {
            List<org.apache.doris.flink.rest.models.Field> copiedFields = new ArrayList<>();
            for (org.apache.doris.flink.rest.models.Field field : fields) {
                copiedFields.add(
                        new org.apache.doris.flink.rest.models.Field(
                                field.getName(),
                                field.getType(),
                                field.getComment(),
                                field.getPrecision(),
                                field.getScale(),
                                field.getAggregationType()));
            }
            return copiedFields;
        }

        private static org.apache.doris.flink.rest.models.Field dorisField(
                FieldSchema fieldSchema) {
            return new org.apache.doris.flink.rest.models.Field(
                    fieldSchema.getName(),
                    fieldSchema.getTypeString(),
                    fieldSchema.getComment(),
                    0,
                    0,
                    null);
        }
    }

    private static class PositionFallbackRecordingSchemaChangeManager
            extends RecordingSchemaChangeManager {
        private boolean fallbackNextAddColumn = true;

        @Override
        protected void applySuccessfulAddColumn(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition position) {
            if (fallbackNextAddColumn) {
                fallbackNextAddColumn = false;
                super.applySuccessfulAddColumn(
                        databaseName, tableName, addFieldSchema, AddColumnPosition.last());
            } else {
                super.applySuccessfulAddColumn(databaseName, tableName, addFieldSchema, position);
            }
        }
    }

    private static class StaleAddColumnRecordingSchemaChangeManager
            extends RecordingSchemaChangeManager {
        @Override
        protected void applySuccessfulAddColumn(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition position) {
            // Simulates Doris accepting the DDL but FE schema lookup never showing the new column.
        }
    }

    private static class StaleModifyColumnRecordingSchemaChangeManager
            extends RecordingSchemaChangeManager {
        @Override
        protected void applySuccessfulModifyColumn(
                String databaseName, String tableName, FieldSchema field) {
            // Simulates Doris accepting the DDL but FE schema lookup still returning the old type.
        }
    }

    private static class StaleDropColumnRecordingSchemaChangeManager
            extends RecordingSchemaChangeManager {
        @Override
        protected void applySuccessfulDropColumn(
                String databaseName, String tableName, String columnName) {
            // Simulates Doris accepting the DDL but FE schema lookup still returning the old
            // column.
        }
    }

    private static class StaleRenameColumnRecordingSchemaChangeManager
            extends RecordingSchemaChangeManager {
        @Override
        protected void applySuccessfulRenameColumn(
                String databaseName, String tableName, String oldColumnName, String newColumnName) {
            // Simulates Doris accepting the DDL but FE schema lookup still returning the old name.
        }
    }

    private static class ModifiedColumn {
        private final String columnName;
        private final String columnType;
        private final String defaultValue;
        private final String comment;

        private ModifiedColumn(FieldSchema fieldSchema) {
            this.columnName = fieldSchema.getName();
            this.columnType = fieldSchema.getTypeString();
            this.defaultValue = fieldSchema.getDefaultValue();
            this.comment = fieldSchema.getComment();
        }
    }

    private static class AddedColumn {
        private final String columnName;
        private final String columnType;
        private final String defaultValue;
        private final AddColumnPosition.PositionType positionType;
        private final String referenceColumn;

        private AddedColumn(FieldSchema fieldSchema, AddColumnPosition position) {
            this.columnName = fieldSchema.getName();
            this.columnType = fieldSchema.getTypeString();
            this.defaultValue = fieldSchema.getDefaultValue();
            this.positionType = position.getPositionType();
            this.referenceColumn = position.getReferenceColumn();
        }
    }
}
