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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSchemaUtils;

import javax.annotation.Nullable;

import java.util.List;

/** {@link MetadataAccessor} for {@link MongoDBDataSource}. */
public class MongoDBMetadataAccessor implements MetadataAccessor {
    private final MongoDBSourceConfig sourceConfig;
    private final SchemaParseMode schemaParseMode;

    public MongoDBMetadataAccessor(
            MongoDBSourceConfig sourceConfig, SchemaParseMode schemaParseMode) {
        this.sourceConfig = sourceConfig;
        this.schemaParseMode = schemaParseMode;
    }

    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by MongoDB.");
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return MongoDBSchemaUtils.listDatabases(sourceConfig);
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        return MongoDBSchemaUtils.listTables(sourceConfig, schemaName);
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (schemaParseMode == SchemaParseMode.SCHEMA_LESS) {
            return MongoDBSchemaUtils.getTableSchema(sourceConfig, tableId);
        }
        throw new UnsupportedOperationException("Unsupported yet.");
    }
}
