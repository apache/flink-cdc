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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogException;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogFactory;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseMySQLCatalog;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Supports {@link OceanBaseDataSink} to schema evolution. */
public class OceanBaseMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMetadataApplier.class);

    private final OceanBaseConnectorOptions connectorOptions;
    private final Configuration config;
    private final OceanBaseMySQLCatalog catalog;

    public OceanBaseMetadataApplier(
            OceanBaseConnectorOptions connectorOptions, Configuration config) throws Exception {
        this.connectorOptions = connectorOptions;
        this.config = config;
        this.catalog =
                (OceanBaseMySQLCatalog)
                        OceanBaseCatalogFactory.createOceanBaseCatalog(connectorOptions);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            // send schema change op to doris
            if (event instanceof CreateTableEvent) {
                applyCreateTableEvent((CreateTableEvent) event);
            } else if (event instanceof AddColumnEvent) {
                applyAddColumnEvent((AddColumnEvent) event);
            } else if (event instanceof DropColumnEvent) {
                applyDropColumnEvent((DropColumnEvent) event);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumnEvent((RenameColumnEvent) event);
            } else if (event instanceof AlterColumnTypeEvent) {
                throw new RuntimeException("Unsupported schema change event, " + event);
            }
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to schema change, " + event + ", reason: " + ex.getMessage());
        }
    }

    private void applyCreateTableEvent(CreateTableEvent createTableEvent)
            throws IllegalArgumentException {
        Schema schema = createTableEvent.getSchema();
        TableId tableId = createTableEvent.tableId();
        OceanBaseTable oceanBaseTable = OceanBaseUtils.toOceanBaseTable(tableId, schema);

        if (!catalog.databaseExists(oceanBaseTable.getDatabaseName())) {
            catalog.createDatabase(oceanBaseTable.getDatabaseName(), true);
        }

        try {
            catalog.createTable(oceanBaseTable, true);
            LOG.info("Successful to create table, event: {}", createTableEvent);
        } catch (OceanBaseCatalogException e) {
            LOG.error("Failed to create table, event: {}", createTableEvent.tableId(), e);
            throw new RuntimeException("Failed to create table, event: " + createTableEvent, e);
        }
    }

    private void applyAddColumnEvent(AddColumnEvent addColumnEvent) {
        List<OceanBaseColumn> addColumns = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                addColumnEvent.getAddedColumns()) {
            // we will ignore position information, and always add the column to the last.
            // The reason is that the order of columns between source table and OceanBase
            // table may be not consistent because of limitations of OceanBase table, so the
            // position may be meaningless. For example, primary keys of OceanBase table
            // must be at the front, but mysql doest not have this limitation, so the order
            // may be different, and also FIRST position is not allowed for OceanBase primary
            // key table.
            Column column = columnWithPosition.getAddColumn();
            OceanBaseColumn.Builder builder =
                    new OceanBaseColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(-1)
                            .setColumnComment(column.getComment());
            OceanBaseUtils.toOceanBaseDataType(column, false, builder);
            addColumns.add(builder.build());
        }

        TableId tableId = addColumnEvent.tableId();
        catalog.alterAddColumns(tableId.getSchemaName(), tableId.getTableName(), addColumns);
    }

    private void applyDropColumnEvent(DropColumnEvent dropColumnEvent) {
        // TODO OceanBase plans to support column drop since 3.3 which has not been released.
        // Support it later.
        throw new UnsupportedOperationException("Rename column is not supported currently");
    }

    private void applyRenameColumnEvent(RenameColumnEvent renameColumnEvent) {
        // TODO OceanBase plans to support column rename since 3.3 which has not been released.
        // Support it later.
        throw new UnsupportedOperationException("Rename column is not supported currently");
    }
}
