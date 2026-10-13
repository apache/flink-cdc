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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;

import javax.annotation.Nullable;

import java.util.List;

/** {@link MetadataAccessor} for {@link Db2DataSource}. */
@Internal
public class Db2MetadataAccessor implements MetadataAccessor {

    private final Db2SourceConfig sourceConfig;

    public Db2MetadataAccessor(Db2SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    /**
     * List all databases from DB2.
     *
     * @return The list of database names
     */
    @Override
    public List<String> listNamespaces() {
        return Db2SchemaUtils.listNamespaces(sourceConfig);
    }

    /**
     * List all schemas from a DB2 database.
     *
     * @param namespace The database name to list schemas from.
     * @return The list of schema names
     */
    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return Db2SchemaUtils.listSchemas(sourceConfig, namespace);
    }

    /**
     * List tables from DB2.
     *
     * @param namespace The database name. If null, uses the configured database.
     * @param schemaName The schema name. If null, lists tables from all schemas.
     * @return The list of {@link TableId}s.
     */
    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        return Db2SchemaUtils.listTables(sourceConfig, namespace, schemaName);
    }

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    @Override
    public Schema getTableSchema(TableId tableId) {
        return Db2SchemaUtils.getTableSchema(sourceConfig, tableId);
    }
}
