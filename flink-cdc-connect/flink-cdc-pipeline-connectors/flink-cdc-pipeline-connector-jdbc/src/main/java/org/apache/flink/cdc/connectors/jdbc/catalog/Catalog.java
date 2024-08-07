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

package org.apache.flink.cdc.connectors.jdbc.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.io.Serializable;
import java.util.List;

/** Database catalog interface. */
public interface Catalog extends Serializable {
    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     *
     * @throws CatalogException in case of any runtime exception
     */
    void close() throws CatalogException;

    /**
     * Create a new table in this catalog.
     *
     * @throws TableAlreadyExistException
     * @throws DatabaseNotExistException
     * @throws CatalogException
     */
    void createTable(TableId tableId, Schema schema, boolean b)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException;

    /**
     * Add a new column in this catalog.
     *
     * @param tableId
     * @param addedColumns
     * @throws TableNotExistException
     * @throws CatalogException
     */
    void addColumn(TableId tableId, AddColumnEvent addedColumns)
            throws TableNotExistException, CatalogException;

    /**
     * Drop a column in this catalog.
     *
     * @param tableId
     * @param addedColumns
     * @throws TableNotExistException
     * @throws CatalogException
     */
    void dropColumn(TableId tableId, DropColumnEvent addedColumns)
            throws TableNotExistException, CatalogException;

    /**
     * Rename a column in this catalog.
     *
     * @param tableId
     * @param addedColumns
     * @throws TableNotExistException
     * @throws CatalogException
     */
    void renameColumn(TableId tableId, RenameColumnEvent addedColumns)
            throws TableNotExistException, CatalogException;

    /**
     * Alter column type in this catalog.
     *
     * @param tableId
     * @param addedColumns
     * @throws TableNotExistException
     * @throws CatalogException
     */
    void alterColumnTyp(TableId tableId, AlterColumnTypeEvent addedColumns)
            throws TableNotExistException, CatalogException;

    /**
     * Get upsert statement.
     *
     * @param tableId
     * @param schema
     * @return
     */
    String getUpsertStatement(TableId tableId, Schema schema);

    /**
     * Get delete statement.
     *
     * @param tableId
     * @param schema
     * @return
     */
    String getDeleteStatement(TableId tableId, List<String> primaryKeys);
}
