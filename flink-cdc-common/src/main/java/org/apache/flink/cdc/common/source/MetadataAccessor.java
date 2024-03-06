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

package org.apache.flink.cdc.common.source;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.List;

/**
 * {@code MetadataAccessor} is used by {@link DataSource} to access the metadata from external
 * systems.
 */
@PublicEvolving
public interface MetadataAccessor {

    /**
     * List all namespaces from external systems.
     *
     * @return The list of namespaces
     * @throws UnsupportedOperationException Thrown, if the external system does not support
     *     namespace.
     */
    List<String> listNamespaces();

    /**
     * List all schemas from external systems.
     *
     * @param namespace The namespace to list schemas from. If null, list schemas from all
     *     namespaces.
     * @return The list of schemas
     * @throws UnsupportedOperationException Thrown, if the external system does not support schema.
     */
    List<String> listSchemas(@Nullable String namespace);

    /**
     * List tables by namespace and schema from external systems.
     *
     * @param namespace The namespace to list tables from. If null, list tables from all namespaces.
     * @param schemaName The schema to list tables from. If null, list tables from all schemas.
     * @return The list of {@link TableId}s.
     */
    List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName);

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    Schema getTableSchema(TableId tableId);
}
