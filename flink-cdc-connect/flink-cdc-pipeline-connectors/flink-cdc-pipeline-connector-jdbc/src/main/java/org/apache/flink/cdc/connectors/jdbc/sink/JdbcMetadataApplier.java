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

package org.apache.flink.cdc.connectors.jdbc.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.jdbc.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that implements the MetadataApplier interface, used to apply JDBC metadata. */
public class JdbcMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataApplier.class);

    // Catalog is unSerializable.
    private final Catalog catalog;

    public JdbcMetadataApplier(Catalog catalog) {
        this.catalog = catalog;
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
                applyAlterColumnType((AlterColumnTypeEvent) event);
            }
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to schema change, " + event + ", reason: " + ex.getMessage());
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event) throws TableNotExistException {
        catalog.renameColumn(event.tableId(), event);
    }

    private void applyDropColumnEvent(DropColumnEvent event) throws TableNotExistException {
        catalog.dropColumn(event.tableId(), event);
    }

    private void applyAddColumnEvent(AddColumnEvent event) throws TableNotExistException {
        catalog.addColumn(event.tableId(), event);
    }

    private void applyCreateTableEvent(CreateTableEvent event)
            throws TableAlreadyExistException, DatabaseNotExistException {
        catalog.createTable(event.tableId(), event.getSchema(), true);
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) throws TableNotExistException {
        catalog.alterColumnTyp(event.tableId(), event);
    }
}
