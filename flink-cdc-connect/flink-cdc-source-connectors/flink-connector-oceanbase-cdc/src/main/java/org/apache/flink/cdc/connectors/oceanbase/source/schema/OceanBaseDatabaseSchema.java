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

package org.apache.flink.cdc.connectors.oceanbase.source.schema;

import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.converter.OceanBaseValueConverters;

import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;

/** OceanBase database schema. */
public class OceanBaseDatabaseSchema extends RelationalDatabaseSchema {

    public OceanBaseDatabaseSchema(
            OceanBaseConnectorConfig connectorConfig,
            Tables.TableFilter tableFilter,
            boolean tableIdCaseInsensitive) {
        super(
                connectorConfig,
                TopicSelector.defaultSelector(
                        connectorConfig,
                        (tableId, prefix, delimiter) ->
                                String.join(delimiter, prefix, tableId.identifier())),
                tableFilter,
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        new OceanBaseValueConverters(connectorConfig),
                        connectorConfig.schemaNameAdjustmentMode().createAdjuster(),
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames(),
                        false),
                tableIdCaseInsensitive,
                connectorConfig.getKeyMapper());
    }
}
