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

package org.apache.flink.cdc.connectors.postgres.source.schema;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Extends PostgresSchema to dispatch Relation messages as schema change events via the event queue
 * and expose buildAndRegisterSchema as public.
 */
public class RelationAwarePostgresSchema extends PostgresSchema {

    private SchemaDispatcher dispatcher;

    public RelationAwarePostgresSchema(
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            PostgresDefaultValueConverter defaultValueConverter,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter) {
        super(config, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
    }

    public void setDispatcher(SchemaDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        super.applySchemaChangesForTable(relationId, table);
        if (dispatcher != null && !isFilteredOut(table.id())) {
            dispatcher.dispatch(table);
        }
    }

    @Override
    public void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }
}
