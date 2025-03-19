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
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseMySQLTestBase;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.oceanbase.connector.flink.utils.OceanBaseJdbcUtils.getTableRowsCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests for {@link OceanBaseMetadataApplier}. */
public class OceanBaseMetadataApplierTest extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMetadataApplierTest.class);

    private OceanBaseMetadataApplier metadataApplier;

    @Before
    public void setup() throws Exception {
        final ImmutableMap<String, String> configMap =
                ImmutableMap.<String, String>builder()
                        .put("url", OB_SERVER.getJdbcUrl())
                        .put("username", OB_SERVER.getUsername())
                        .put("password", OB_SERVER.getPassword())
                        .build();
        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(configMap);
        metadataApplier = new OceanBaseMetadataApplier(connectorOptions);
    }

    @After
    public void close() throws Exception {
        if (metadataApplier != null) {
            metadataApplier.close();
            metadataApplier = null;
        }
    }

    @Test
    public void testCreateTable() {
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
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

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

        assertEquals(expectTable, actualTable);
    }

    @Test
    public void testDropTable() {
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
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        DropTableEvent dropTableEvent = new DropTableEvent(tableId);
        metadataApplier.applySchemaChange(dropTableEvent);
        OceanBaseTable actualDropTable =
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNull(actualDropTable);
    }

    @Test
    public void testTruncateTable() throws SQLException {
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
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        String insertSQL =
                String.format("insert into %s values(1, true, now())", tableId.identifier());
        execute(insertSQL);
        assertEquals(1, getTableRowsCount(this::getConnection, tableId.identifier()));

        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(tableId);
        metadataApplier.applySchemaChange(truncateTableEvent);

        OceanBaseTable changeTable =
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(changeTable);
        assertEquals(0, getTableRowsCount(this::getConnection, tableId.identifier()));
    }

    @Test
    public void testDropColumnEvent() {
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
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        DropColumnEvent dropColumnEvent = new DropColumnEvent(tableId, Lists.newArrayList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);

        OceanBaseTable changeTable =
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(changeTable);
        assertNotNull(changeTable.getColumns());
        assertEquals(actualTable.getColumns().size() - 1, changeTable.getColumns().size());
    }

    @Test
    public void testAlterColumnType() {
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
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);
        assertFalse(actualTable.getColumns().isEmpty());

        ImmutableMap<String, DataType> map = ImmutableMap.of("col2", new IntType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, map);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        OceanBaseTable changeTable =
                getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(changeTable);
        assertFalse(changeTable.getColumns().isEmpty());

        assertNotEquals(
                actualTable.getColumn("col2").getDataType(),
                changeTable.getColumn("col2").getDataType());
    }
}
