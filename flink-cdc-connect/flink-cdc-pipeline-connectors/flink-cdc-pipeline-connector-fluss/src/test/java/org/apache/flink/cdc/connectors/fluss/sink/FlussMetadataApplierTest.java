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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.IntType;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.exception.InvalidConfigException;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussMetaDataApplier}. */
public class FlussMetadataApplierTest {
    @RegisterExtension
    private static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static Connection conn;
    private static Admin admin;
    private static final String DATABASE_NAME = "test_database";

    @BeforeAll
    static void setup() {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @AfterEach
    void cleanup() throws ExecutionException, InterruptedException {
        admin.dropDatabase(DATABASE_NAME, true, true).get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreateTableAllTypes(boolean primaryKeyTable) throws Exception {
        String[] fieldNames =
                new String[] {
                    "binary_col",
                    "varbinary_col",
                    "bytes_col",
                    "boolean_col",
                    "tinyint_col",
                    "smallint_col",
                    "int_col",
                    "bigint_col",
                    "float_col",
                    "double_col",
                    "decimal_col",
                    "char_col",
                    "varchar_col",
                    "string_col",
                    "date_col",
                    "time_col",
                    "timestamp_col",
                    "timestamp_ltz_col",
                };

        org.apache.flink.cdc.common.types.DataType[] cdcDataTypes =
                new org.apache.flink.cdc.common.types.DataType[] {
                    DataTypes.BINARY(10),
                    DataTypes.VARBINARY(10),
                    DataTypes.BYTES(),
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    new IntType(false),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(38, 18),
                    DataTypes.CHAR(10),
                    DataTypes.VARCHAR(100),
                    DataTypes.STRING(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP_LTZ(6)
                };

        com.alibaba.fluss.types.DataType[] flussDataTypes =
                new com.alibaba.fluss.types.DataType[] {
                    com.alibaba.fluss.types.DataTypes.BINARY(10),
                    // fluss not support binary, will be mapped to bytes
                    com.alibaba.fluss.types.DataTypes.BYTES(),
                    com.alibaba.fluss.types.DataTypes.BYTES(),
                    com.alibaba.fluss.types.DataTypes.BOOLEAN(),
                    com.alibaba.fluss.types.DataTypes.TINYINT(),
                    com.alibaba.fluss.types.DataTypes.SMALLINT(),
                    new com.alibaba.fluss.types.IntType(false),
                    com.alibaba.fluss.types.DataTypes.BIGINT(),
                    com.alibaba.fluss.types.DataTypes.FLOAT(),
                    com.alibaba.fluss.types.DataTypes.DOUBLE(),
                    com.alibaba.fluss.types.DataTypes.DECIMAL(38, 18),
                    com.alibaba.fluss.types.DataTypes.CHAR(10),
                    // fluss not support varchar, will be mapped to string
                    com.alibaba.fluss.types.DataTypes.STRING(),
                    com.alibaba.fluss.types.DataTypes.STRING(),
                    com.alibaba.fluss.types.DataTypes.DATE(),
                    com.alibaba.fluss.types.DataTypes.TIME(),
                    com.alibaba.fluss.types.DataTypes.TIMESTAMP(3),
                    com.alibaba.fluss.types.DataTypes.TIMESTAMP_LTZ(6)
                };

        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            // apply create-table event
            Schema.Builder builder = Schema.newBuilder();
            for (int i = 0; i < fieldNames.length; i++) {
                builder.physicalColumn(fieldNames[i], cdcDataTypes[i]);
            }
            if (primaryKeyTable) {
                builder.primaryKey("int_col");
            }
            Schema schema = builder.build();
            TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // check table
            TableInfo tableInfo = admin.getTableInfo(new TablePath(DATABASE_NAME, "table1")).get();
            RowType flussRowType = tableInfo.getRowType();
            for (int i = 0; i < fieldNames.length; i++) {
                assertThat(flussRowType.getTypeAt(i)).isEqualTo(flussDataTypes[i]);
                assertThat(flussRowType.getFieldNames().get(i)).isEqualTo(fieldNames[i]);
            }
            assertThat(tableInfo.hasPrimaryKey()).isEqualTo(primaryKeyTable);
            if (primaryKeyTable) {
                assertThat(tableInfo.getPrimaryKeys()).containsExactly("int_col");
            }
        }
    }

    @Test
    void testUnsupportedType() throws Exception {
        String[] fieldNames = new String[] {"timestamp_tz_col", "array_col", "map_col", "row_col"};

        org.apache.flink.cdc.common.types.DataType[] cdcDataTypes =
                new org.apache.flink.cdc.common.types.DataType[] {
                    DataTypes.ARRAY(DataTypes.STRING()),
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                    DataTypes.ROW(
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("age", DataTypes.INT())),
                    DataTypes.TIMESTAMP_TZ()
                };

        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            for (int i = 0; i < fieldNames.length; i++) {
                // apply create-table event
                Schema.Builder builder = Schema.newBuilder();
                builder.physicalColumn(fieldNames[i], cdcDataTypes[i]);
                Schema schema = builder.build();
                TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
                CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
                assertThatThrownBy(() -> applier.applySchemaChange(createTableEvent))
                        .rootCause()
                        .isExactlyInstanceOf(UnsupportedOperationException.class);
            }
        }
    }

    @Test
    void testDropTableEvent() throws Exception {
        TablePath tablePath = new TablePath("fluss", "table1");
        admin.createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        com.alibaba.fluss.metadata.Schema.newBuilder()
                                                .column(
                                                        "id",
                                                        com.alibaba.fluss.types.DataTypes.INT())
                                                .build())
                                .build(),
                        true)
                .get();
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            assertThat(admin.tableExists(tablePath).get()).isTrue();
            applier.applySchemaChange(
                    new DropTableEvent(
                            TableId.tableId(
                                    tablePath.getDatabaseName(), tablePath.getTableName())));
            assertThat(admin.tableExists(tablePath).get()).isFalse();
        }
    }

    @Test
    void testCreatePartitionedTable() throws Exception {
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            // apply create-table event
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .primaryKey("id", "name")
                            .partitionKey("name")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableInfo tableInfo = admin.getTableInfo(new TablePath(DATABASE_NAME, "table1")).get();
            assertThat(tableInfo.getPartitionKeys()).containsExactly("name");
        }
    }

    @Test
    void testPartitionKeyIsNotSubsetOfPrimaryKeys() throws Exception {
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            // apply create-table event
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .primaryKey("id")
                            .partitionKey("name")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            assertThatThrownBy(() -> applier.applySchemaChange(createTableEvent))
                    .rootCause()
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Partitioned Primary Key Table requires partition key [name] is a subset of the primary key [id]");
        }
    }

    @Test
    void testBuckNumber() throws Exception {
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.singletonMap(DATABASE_NAME + ".table1", 3))) {
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .build();
            TableId tableId1 = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
            TableId tableId2 = TableId.tableId("default_namespace", DATABASE_NAME, "table2");
            CreateTableEvent createTableEvent1 = new CreateTableEvent(tableId1, schema);
            CreateTableEvent createTableEvent2 = new CreateTableEvent(tableId2, schema);
            applier.applySchemaChange(createTableEvent1);
            applier.applySchemaChange(createTableEvent2);
            assertThat(
                            admin.getTableInfo(new TablePath(DATABASE_NAME, "table1"))
                                    .get()
                                    .getNumBuckets())
                    .isEqualTo(3);
            // if not set bucket.num, will use the default buck number which is 1.
            assertThat(
                            admin.getTableInfo(new TablePath(DATABASE_NAME, "table2"))
                                    .get()
                                    .getNumBuckets())
                    .isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBucketKey(boolean primaryKeyTable) throws Exception {
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.singletonMap(
                                DATABASE_NAME + ".table1", Collections.singletonList("id")),
                        Collections.emptyMap())) {
            Schema.Builder builder =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING());
            if (primaryKeyTable) {
                builder.primaryKey("name", "id");
            }
            Schema schema = builder.build();
            TableId tableId1 = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
            TableId tableId2 = TableId.tableId("default_namespace", DATABASE_NAME, "table2");
            CreateTableEvent createTableEvent1 = new CreateTableEvent(tableId1, schema);
            CreateTableEvent createTableEvent2 = new CreateTableEvent(tableId2, schema);
            applier.applySchemaChange(createTableEvent1);
            applier.applySchemaChange(createTableEvent2);
            assertThat(
                            admin.getTableInfo(new TablePath(DATABASE_NAME, "table1"))
                                    .get()
                                    .getBucketKeys())
                    .containsExactly("id");
            // if not set bucket.keys, primary key table will use (primary keys - partition keys) as
            // bucket keys.
            if (primaryKeyTable) {
                assertThat(
                                admin.getTableInfo(new TablePath(DATABASE_NAME, "table2"))
                                        .get()
                                        .getBucketKeys())
                        .containsExactlyInAnyOrder("id", "name");
            } else {
                assertThat(
                                admin.getTableInfo(new TablePath(DATABASE_NAME, "table2"))
                                        .get()
                                        .getBucketKeys())
                        .isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBucketKeyNotSetOfPrimaryKeys(boolean partitionTables) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id", "name");
        if (partitionTables) {
            builder.partitionKey("name");
        }
        Schema schema = builder.build();
        Map<String, List<String>> bucketKeys = new HashMap<>();
        // table1 is designed to test bucket.keys is not allowed to include any column in partition
        // keys.
        // table2 and table 3 is designed to test bucket.keys is must be a subset of primary keys
        // excluding partition keys.
        // table4 is designed to test primary key table will use (primary keys - partition keys) as
        // bucket keys if not set bucket.keys.
        bucketKeys.put(DATABASE_NAME + ".table1", Collections.singletonList("name"));
        bucketKeys.put(DATABASE_NAME + ".table2", Collections.singletonList("age"));
        bucketKeys.put(DATABASE_NAME + ".table3", Collections.singletonList("id"));
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        bucketKeys,
                        Collections.emptyMap())) {
            if (partitionTables) {
                assertThatThrownBy(
                                () ->
                                        applier.applySchemaChange(
                                                new CreateTableEvent(
                                                        TableId.tableId(DATABASE_NAME, "table1"),
                                                        schema)))
                        .hasMessageContaining(
                                "Bucket key [name] shouldn't include any column in partition keys [name]");
            } else {
                applier.applySchemaChange(
                        new CreateTableEvent(TableId.tableId(DATABASE_NAME, "table1"), schema));
                assertThat(
                                admin.getTableInfo(new TablePath(DATABASE_NAME, "table1"))
                                        .get()
                                        .getBucketKeys())
                        .containsExactly("name");
            }
            assertThatThrownBy(
                            () ->
                                    applier.applySchemaChange(
                                            new CreateTableEvent(
                                                    TableId.tableId(DATABASE_NAME, "table2"),
                                                    schema)))
                    .hasMessageContaining(
                            "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables");
            applier.applySchemaChange(
                    new CreateTableEvent(TableId.tableId(DATABASE_NAME, "table3"), schema));
            assertThat(
                            admin.getTableInfo(new TablePath(DATABASE_NAME, "table3"))
                                    .get()
                                    .getBucketKeys())
                    .containsExactly("id");

            applier.applySchemaChange(
                    new CreateTableEvent(TableId.tableId(DATABASE_NAME, "table4"), schema));
            // if not set bucket.keys, primary key table will use (primary keys - partition keys) as
            // bucket keys.
            if (partitionTables) {
                assertThat(
                                admin.getTableInfo(new TablePath(DATABASE_NAME, "table4"))
                                        .get()
                                        .getBucketKeys())
                        .containsExactly("id");
            } else {
                assertThat(
                                admin.getTableInfo(new TablePath(DATABASE_NAME, "table4"))
                                        .get()
                                        .getBucketKeys())
                        .containsExactlyInAnyOrder("name", "id");
            }
        }
    }

    @Test
    void testRecreateTableWithDifferentSchema() throws Exception {
        TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id", "name")
                        .partitionKey("name")
                        .build();
        Schema differentSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("age", DataTypes.INT())
                        .build();
        // Use another object for comparison.
        Schema sameSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id", "name")
                        .partitionKey("name")
                        .build();
        // create table with schema1
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            applier.applySchemaChange(new CreateTableEvent(tableId, initialSchema));
        }

        // recreate table with schema2 again
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            assertThatThrownBy(
                            () ->
                                    applier.applySchemaChange(
                                            new CreateTableEvent(tableId, differentSchema)))
                    .hasMessageContaining(
                            "The table schema inffered by Flink CDC is not matched with current Fluss table schema");
        }

        // recreate table with schema1 again
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            applier.applySchemaChange(new CreateTableEvent(tableId, sameSchema));
        }
    }

    @Test
    void testCreateTableWithTableOptions() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id", "name")
                        .partitionKey("name")
                        .build();
        TableId tableId = TableId.tableId("default_namespace", DATABASE_NAME, "table1");
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.singletonMap("key", "value"),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            assertThatThrownBy(() -> applier.applySchemaChange(createTableEvent))
                    .rootCause()
                    .isExactlyInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("'key' is not a Fluss table property");
        }
        try (FlussMetaDataApplier applier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.singletonMap(TABLE_REPLICATION_FACTOR.key(), "5"),
                        Collections.emptyMap(),
                        Collections.emptyMap())) {
            applier.applySchemaChange(createTableEvent);
            TableInfo tableInfo = admin.getTableInfo(new TablePath(DATABASE_NAME, "table1")).get();
            assertThat(tableInfo.getProperties().get(TABLE_REPLICATION_FACTOR)).isEqualTo(5);
        }
    }
}
