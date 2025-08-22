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

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import org.apache.flink.cdc.common.types.DataType;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link OceanBaseCatalog} for OceanBase connector that supports schema evolution under Oracle
 * mode.
 */
public class OceanBaseOracleCatalog extends OceanBaseCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseOracleCatalog.class);

    public OceanBaseOracleCatalog(OceanBaseConnectorOptions connectorOptions) {
        super(connectorOptions);
    }

    @Override
    public boolean databaseExists(String databaseName) throws OceanBaseCatalogException {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws OceanBaseCatalogException {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws OceanBaseCatalogException {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void createTable(OceanBaseTable table, boolean ignoreIfExists)
            throws OceanBaseCatalogException {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void alterAddColumns(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void alterDropColumns(String schemaName, String tableName, List<String> dropColumns) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void alterColumnType(
            String schemaName, String tableName, String columnName, DataType dataType) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void renameColumn(
            String schemaName, String tableName, String oldColumnName, String newColumnName) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void dropTable(String schemaName, String tableName) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }

    @Override
    public void truncateTable(String schemaName, String tableName) {
        throw new OceanBaseCatalogException(
                "This operation under oracle tenant is not supported currently.");
    }
}
