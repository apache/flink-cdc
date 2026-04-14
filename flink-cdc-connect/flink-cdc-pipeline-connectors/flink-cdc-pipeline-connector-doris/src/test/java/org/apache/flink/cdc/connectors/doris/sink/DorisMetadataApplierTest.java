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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
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
                new DorisMetadataApplier(
                        createDorisOptions(),
                        Configuration.fromMap(Collections.emptyMap()),
                        schemaChangeManager,
                        (dorisOptions, tableId) ->
                                createDorisSchema("DEPLOY_MODE", "project_id", "job_name"));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.STRING())
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
                                        "DEPLOY_MODE",
                                        "project_id",
                                        "job_name",
                                        "CONFLUENT__LAST_UPDATED"));

        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("DEPLOY_MODE", DataTypes.INT())
                        .physicalColumn("project_id", DataTypes.BIGINT())
                        .physicalColumn("job_name", DataTypes.STRING())
                        .physicalColumn("CONFLUENT__LAST_UPDATED", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, targetSchema));

        Assertions.assertThat(schemaChangeManager.createTableInvocations).isZero();
        Assertions.assertThat(schemaChangeManager.addedColumns).isEmpty();
        Assertions.assertThat(applier.getCachedSchema(TABLE_ID)).isEqualTo(targetSchema);
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

    private static class RecordingSchemaChangeManager extends DorisSchemaChangeManager {
        private int createTableInvocations;
        private final List<AddedColumn> addedColumns = new ArrayList<>();

        private RecordingSchemaChangeManager() {
            super(createDorisOptions(), null);
        }

        @Override
        public boolean createTable(org.apache.doris.flink.catalog.doris.TableSchema tableSchema) {
            createTableInvocations++;
            return true;
        }

        @Override
        public boolean addColumn(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition addColumnPosition) {
            addedColumns.add(new AddedColumn(addFieldSchema));
            return true;
        }
    }

    private static class AddedColumn {
        private final String columnName;
        private final String columnType;

        private AddedColumn(FieldSchema fieldSchema) {
            this.columnName = fieldSchema.getName();
            this.columnType = fieldSchema.getTypeString();
        }
    }
}
