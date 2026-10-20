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

package org.apache.flink.cdc.connectors.fluss.source;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.fluss.utils.FlussConversions;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link MetadataAccessor} implementation that uses the Fluss Admin API to list databases,
 * tables, and retrieve table schemas from a Fluss cluster.
 */
public class FlussMetadataAccessor implements MetadataAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(FlussMetadataAccessor.class);

    private final Configuration flussConfig;

    public FlussMetadataAccessor(Configuration flussConfig) {
        this.flussConfig = flussConfig;
    }

    @Override
    public List<String> listNamespaces() {
        return Collections.emptyList();
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig)) {
            Admin admin = connection.getAdmin();
            return admin.listDatabases().get();
        } catch (Exception e) {
            LOG.error("Failed to list Fluss databases", e);
            throw new RuntimeException("Failed to list Fluss databases", e);
        }
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig)) {
            Admin admin = connection.getAdmin();
            List<TableId> tableIds = new ArrayList<>();

            if (schemaName != null) {
                List<String> tables = admin.listTables(schemaName).get();
                for (String tableName : tables) {
                    tableIds.add(TableId.tableId(schemaName, tableName));
                }
            } else {
                List<String> databases = admin.listDatabases().get();
                for (String database : databases) {
                    List<String> tables = admin.listTables(database).get();
                    for (String tableName : tables) {
                        tableIds.add(TableId.tableId(database, tableName));
                    }
                }
            }
            return tableIds;
        } catch (Exception e) {
            LOG.error("Failed to list Fluss tables", e);
            throw new RuntimeException("Failed to list Fluss tables", e);
        }
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig)) {
            Admin admin = connection.getAdmin();
            TablePath tablePath = new TablePath(tableId.getSchemaName(), tableId.getTableName());
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            return FlussConversions.toCdcSchema(
                    tableInfo.getSchema(), tableInfo.getPartitionKeys());
        } catch (Exception e) {
            LOG.error("Failed to get Fluss table schema for {}", tableId, e);
            throw new RuntimeException("Failed to get Fluss table schema for " + tableId, e);
        }
    }
}
