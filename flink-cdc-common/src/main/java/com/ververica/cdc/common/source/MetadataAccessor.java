/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.source;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.List;

/**
 * {@code MetadataAccessor} is used by {@link DataSource} to access the metadata from external
 * systems.
 */
@PublicEvolving
public interface MetadataAccessor {

    /** List all namespaces from external systems. */
    List<String> listNamespaces() throws UnsupportedOperationException;

    /** List schemas by namespace from external systems. */
    List<String> listSchemas(@Nullable String namespace) throws UnsupportedOperationException;

    /** List tables by namespace and schema from external systems. */
    List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName);

    /** Get the schema of the given table. */
    Schema getTableSchema(TableId tableId);
}
