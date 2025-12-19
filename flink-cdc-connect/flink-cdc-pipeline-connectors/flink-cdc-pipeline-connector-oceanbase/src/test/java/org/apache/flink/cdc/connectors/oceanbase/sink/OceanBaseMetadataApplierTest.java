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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestUtils;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseContainer;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseTestMySQLCatalog;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OceanBaseMetadataApplier}. */
class OceanBaseMetadataApplierTest {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMetadataApplierTest.class);

    private OceanBaseMetadataApplier metadataApplier;
    private OceanBaseTestMySQLCatalog catalog;

    private static final Network NETWORK = Network.newNetwork();

    private static final OceanBaseContainer OB_SERVER =
            OceanBaseTestUtils.createOceanBaseContainerForJdbc()
                    .withNetwork(NETWORK)
                    .withNetworkAliases("oceanbase")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    static void startContainers() {
        Startables.deepStart(OB_SERVER).join();
    }

    @AfterAll
    static void stopContainers() {
        OB_SERVER.close();
    }

    @BeforeEach
    void setup() {
        final ImmutableMap<String, String> configMap =
                ImmutableMap.<String, String>builder()
                        .put("url", OB_SERVER.getJdbcUrl())
                        .put("username", OB_SERVER.getUsername())
                        .put("password", OB_SERVER.getPassword())
                        .build();
        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(configMap);
        metadataApplier = new OceanBaseMetadataApplier(connectorOptions);
        catalog = new OceanBaseTestMySQLCatalog(connectorOptions);
        catalog.open();
    }

    @Test
    void testCreateTable() {
        TableId tableId = TableId.parse("test.tbl1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        List<OceanBaseColumn> columns = new ArrayList<>();
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col1")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNumericScale(0)
                        .setNullable(false)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col2")
                        .setOrdinalPosition(1)
                        .setDataType("tinyint")
                        .setNumericScale(0)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col3")
                        .setOrdinalPosition(2)
                        .setDataType("timestamp")
                        .setNullable(true)
                        .build());
        OceanBaseTable expectTable =
                new OceanBaseTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(OceanBaseTable.TableType.PRIMARY_KEY)
                        .setColumns(columns)
                        .setTableKeys(schema.primaryKeys())
                        .build();

        assertThat(actualTable).isEqualTo(expectTable);

        tableId = TableId.tableId("nonexistent' OR '1'='1", "tabl1");
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();
        assertThat(actualTable.getDatabaseName()).isEqualTo("nonexistent' OR '1'='1");

        tableId = TableId.tableId("test", "nonexistent' OR '1'='1");
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();
        assertThat(actualTable.getTableName()).isEqualTo("nonexistent' OR '1'='1");

        tableId = TableId.tableId("nonexistent` OR `1`=`1", "tabl1");
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();
        assertThat(actualTable.getDatabaseName()).isEqualTo("nonexistent` OR `1`=`1");

        tableId = TableId.tableId("test", "nonexistent` OR `1`=`1");
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();
        assertThat(actualTable.getTableName()).isEqualTo("nonexistent` OR `1`=`1");
    }

    @Test
    void testCreateTableWithAllType() {
        TableId tableId = TableId.parse("test.tbl6");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", DataTypes.BOOLEAN())
                        .physicalColumn("col3", DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn("col4", DataTypes.BYTES())
                        .physicalColumn("col5", DataTypes.TINYINT())
                        .physicalColumn("col6", DataTypes.SMALLINT())
                        .physicalColumn("col7", DataTypes.BIGINT())
                        .physicalColumn("col8", DataTypes.FLOAT())
                        .physicalColumn("col9", DataTypes.DOUBLE())
                        .physicalColumn("col10", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("col11", DataTypes.CHAR(5))
                        .physicalColumn("col12", DataTypes.VARCHAR(10))
                        .physicalColumn("col13", DataTypes.STRING())
                        .physicalColumn("col14", DataTypes.DATE())
                        .physicalColumn("col15", DataTypes.TIME())
                        .physicalColumn("col16", DataTypes.TIME(6))
                        .physicalColumn("col17", DataTypes.TIMESTAMP())
                        .physicalColumn("col18", DataTypes.TIMESTAMP(3))
                        .physicalColumn("col19", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn("col20", DataTypes.TIMESTAMP_TZ())
                        .physicalColumn("col21", DataTypes.TIMESTAMP_TZ(3))
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        List<OceanBaseColumn> columns = new ArrayList<>();
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col1")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNumericScale(0)
                        .setNullable(false)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col2")
                        .setOrdinalPosition(1)
                        .setDataType("tinyint")
                        .setNumericScale(0)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col3")
                        .setOrdinalPosition(2)
                        .setDataType("timestamp")
                        .setNullable(true)
                        .build());

        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col4")
                        .setOrdinalPosition(3)
                        .setDataType("longblob")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col5")
                        .setOrdinalPosition(4)
                        .setDataType("tinyint")
                        .setNumericScale(0)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col6")
                        .setOrdinalPosition(5)
                        .setDataType("smallint")
                        .setNumericScale(0)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col7")
                        .setOrdinalPosition(6)
                        .setDataType("bigint")
                        .setNumericScale(0)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col8")
                        .setOrdinalPosition(7)
                        .setDataType("float")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col9")
                        .setOrdinalPosition(8)
                        .setDataType("double")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col10")
                        .setOrdinalPosition(9)
                        .setDataType("decimal")
                        .setNumericScale(3)
                        .setColumnSize(6)
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col11")
                        .setOrdinalPosition(10)
                        .setDataType("char")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col12")
                        .setOrdinalPosition(11)
                        .setDataType("varchar")
                        .setNullable(true)
                        .build());

        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col13")
                        .setOrdinalPosition(12)
                        .setDataType("text")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col14")
                        .setOrdinalPosition(13)
                        .setDataType("date")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col15")
                        .setOrdinalPosition(14)
                        .setDataType("time")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col16")
                        .setOrdinalPosition(15)
                        .setDataType("time")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col17")
                        .setOrdinalPosition(16)
                        .setDataType("datetime")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col18")
                        .setOrdinalPosition(17)
                        .setDataType("datetime")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col19")
                        .setOrdinalPosition(18)
                        .setDataType("timestamp")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col20")
                        .setOrdinalPosition(19)
                        .setDataType("timestamp")
                        .setNullable(true)
                        .build());
        columns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("col21")
                        .setOrdinalPosition(20)
                        .setDataType("timestamp")
                        .setNullable(true)
                        .build());

        OceanBaseTable expectTable =
                new OceanBaseTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(OceanBaseTable.TableType.PRIMARY_KEY)
                        .setColumns(columns)
                        .setTableKeys(schema.primaryKeys())
                        .build();

        assertThat(actualTable).isEqualTo(expectTable);
    }

    @Test
    void testDropTable() {
        TableId tableId = TableId.parse("test.tbl2");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        DropTableEvent dropTableEvent = new DropTableEvent(tableId);
        metadataApplier.applySchemaChange(dropTableEvent);
        OceanBaseTable actualDropTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualDropTable).isNull();
    }

    @Test
    void testTruncateTable() throws SQLException {
        TableId tableId = TableId.parse("test.tbl3");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        String insertSQL =
                String.format("insert into %s values(1, true, now())", tableId.identifier());
        catalog.executeUpdateStatement(insertSQL);

        TruncateTableEvent dropTableEvent = new TruncateTableEvent(tableId);
        metadataApplier.applySchemaChange(dropTableEvent);

        List<String> values =
                catalog.executeSingleColumnStatement(
                        String.format("select col1 from %s", tableId.identifier()));
        assertThat(values).isEmpty();
    }

    @Test
    void testDropColumnEvent() {
        TableId tableId = TableId.parse("test.tbl4");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        DropColumnEvent dropColumnEvent = new DropColumnEvent(tableId, Lists.newArrayList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);

        OceanBaseTable changeTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);

        assertThat(changeTable).isNotNull();
        assertThat(changeTable.getColumns().size()).isNotEqualTo(actualTable.getColumns().size());
    }

    @Test
    void testAlterColumnType() {
        TableId tableId = TableId.parse("test.tbl5");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        OceanBaseTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertThat(actualTable).isNotNull();

        ImmutableMap<String, DataType> map = ImmutableMap.of("col2", new IntType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, map);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);

        OceanBaseTable changeTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);

        assertThat(changeTable).isNotNull();
        assertThat(changeTable.getColumn("col2").getDataType())
                .isNotEqualTo(actualTable.getColumn("col2").getDataType());
    }
}
