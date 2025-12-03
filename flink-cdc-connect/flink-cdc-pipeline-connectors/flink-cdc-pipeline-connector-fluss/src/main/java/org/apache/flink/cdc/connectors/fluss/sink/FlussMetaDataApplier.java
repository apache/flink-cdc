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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.table.api.ValidationException;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.connectors.fluss.utils.FlussConversions.toFlussTable;

/** {@link MetadataApplier} for fluss. */
public class FlussMetaDataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(FlussMetaDataApplier.class);
    private final Configuration flussClientConfig;
    private final Map<String, String> tableProperties;
    private final Map<String, List<String>> bucketKeysMap;
    private final Map<String, Integer> bucketNumMap;
    private Set<SchemaChangeEventType> enabledEventTypes =
            new HashSet<>(Arrays.asList(CREATE_TABLE, DROP_TABLE));

    public FlussMetaDataApplier(
            Configuration flussClientConfig,
            Map<String, String> tableProperties,
            Map<String, List<String>> bucketKeysMap,
            Map<String, Integer> bucketNumMap) {
        this.flussClientConfig = flussClientConfig;
        this.tableProperties = tableProperties;
        this.bucketKeysMap = bucketKeysMap;
        this.bucketNumMap = bucketNumMap;
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        enabledEventTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledEventTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Arrays.stream(SchemaChangeEventTypeFamily.TABLE).collect(Collectors.toSet());
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        LOG.info("fluss metadata applier receive schemaChangeEvent {}", schemaChangeEvent);
        if (schemaChangeEvent instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
            applyCreateTable(createTableEvent);
        } else if (schemaChangeEvent instanceof DropTableEvent) {
            DropTableEvent dropTableEvent = (DropTableEvent) schemaChangeEvent;
            applyDropTable(dropTableEvent);
        } else {
            throw new IllegalArgumentException(
                    "fluss metadata applier only support CreateTableEvent now but receives "
                            + schemaChangeEvent);
        }
    }

    private void applyCreateTable(CreateTableEvent event) {
        try (Connection connection = ConnectionFactory.createConnection(flussClientConfig);
                Admin admin = connection.getAdmin()) {
            TableId tableId = event.tableId();
            TablePath tablePath = new TablePath(tableId.getSchemaName(), tableId.getTableName());
            String tableIdentifier = tablePath.getDatabaseName() + "." + tablePath.getTableName();
            List<String> bucketKeys = bucketKeysMap.get(tableIdentifier);
            Integer bucketNum = bucketNumMap.get(tableIdentifier);
            TableDescriptor inferredFlussTable =
                    toFlussTable(event.getSchema(), bucketKeys, bucketNum, tableProperties);
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
            if (!admin.tableExists(tablePath).get()) {
                admin.createTable(tablePath, inferredFlussTable, false).get();
            } else {
                TableInfo currentTableInfo = admin.getTableInfo(tablePath).get();
                // sanity check to prevent unexpected table schema evolution.
                sanityCheck(inferredFlussTable, currentTableInfo);
            }
        } catch (Exception e) {
            LOG.error("Failed to apply schema change {}", event, e);
            throw new RuntimeException(e);
        }
    }

    private void applyDropTable(DropTableEvent event) {
        try (Connection connection = ConnectionFactory.createConnection(flussClientConfig);
                Admin admin = connection.getAdmin()) {
            TableId tableId = event.tableId();
            TablePath tablePath = new TablePath(tableId.getSchemaName(), tableId.getTableName());
            admin.dropTable(tablePath, true).get();
        } catch (Exception e) {
            LOG.error("Failed to apply schema change {}", event, e);
            throw new RuntimeException(e);
        }
    }

    private void sanityCheck(TableDescriptor inferredFlussTable, TableInfo currentTableInfo) {
        List<String> inferredPrimaryKeyColumnNames =
                inferredFlussTable.getSchema().getPrimaryKeyColumnNames().stream()
                        .sorted()
                        .collect(Collectors.toList());
        List<String> currentPrimaryKeyColumnNames =
                currentTableInfo.getSchema().getPrimaryKeyColumnNames().stream()
                        .sorted()
                        .collect(Collectors.toList());
        if (!inferredPrimaryKeyColumnNames.equals(currentPrimaryKeyColumnNames)) {
            throw new ValidationException(
                    "The table schema inffered by Flink CDC is not matched with current Fluss table schema. "
                            + "\n New Fluss table's primary keys : "
                            + inferredPrimaryKeyColumnNames
                            + "\n Current Fluss's primary keys: "
                            + currentPrimaryKeyColumnNames);
        }

        List<String> inferredBucketKeys = inferredFlussTable.getBucketKeys();
        List<String> currentBucketKeys = currentTableInfo.getBucketKeys();
        if (!inferredBucketKeys.equals(currentBucketKeys)) {
            throw new ValidationException(
                    "The table schema inffered by Flink CDC is not matched with current Fluss table schema. "
                            + "\n New Fluss table's bucket keys : "
                            + inferredBucketKeys
                            + "\n Current Fluss's bucket keys: "
                            + currentBucketKeys);
        }

        List<String> inferredPartitionKeys = inferredFlussTable.getPartitionKeys();
        List<String> currentPartitionKeys = currentTableInfo.getPartitionKeys();
        if (!inferredPartitionKeys.equals(currentPartitionKeys)) {
            throw new ValidationException(
                    "The table schema inffered by Flink CDC is not matched with current Fluss table schema. "
                            + "\n New Fluss table's partition keys : "
                            + inferredPartitionKeys
                            + "\n Current Fluss's partition keys: "
                            + currentPartitionKeys);
        }
    }
}
