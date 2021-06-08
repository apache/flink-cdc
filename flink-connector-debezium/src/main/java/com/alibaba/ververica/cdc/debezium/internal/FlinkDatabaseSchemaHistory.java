/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.debezium.internal;

import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction.StateUtils;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DatabaseSchema;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.relational.history.TableChanges.TableChange;

/**
 * The {@link FlinkDatabaseSchemaHistory} only stores the snapshot of the current schema of the
 * monitored tables. When recovering from the checkpoint, it should apply all the table to the
 * {@link DatabaseSchema}, which doesn't need to replay the history anymore.
 *
 * <p>Considering the data structure maintained in the {@link FlinkDatabaseSchemaHistory} is much
 * different from the {@link FlinkDatabaseHistory}, it's not compatible with the {@link
 * FlinkDatabaseHistory}. Because it only maintains the snapshot of the tables, it's more efficient
 * to use the memory.
 */
public class FlinkDatabaseSchemaHistory implements DatabaseHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    private final JsonTableChangeSerializer tableChangesSerializer =
            new JsonTableChangeSerializer();

    private ConcurrentMap<TableId, HistoryRecord> tables;
    private String instanceName;
    private DatabaseHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;
    private boolean useCatalogBeforeSchema;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.listener = listener;
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_MONITORED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;

        // recover
        this.tables = new ConcurrentHashMap<>();
        for (HistoryRecord record : StateUtils.retrieveHistory(instanceName)) {
            // validate here
            TableChange tableChange =
                    JsonTableChangeSerializer.fromDocument(
                            record.document(), useCatalogBeforeSchema);
            tables.put(tableChange.getId(), record);
        }
        // register
        StateUtils.registerHistory(instanceName, tables.values());
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws DatabaseHistoryException {
        throw new UnsupportedOperationException(
                "The FlinkDatabaseSchemaHistory needs debezium provides the schema.");
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes)
            throws DatabaseHistoryException {
        final HistoryRecord record =
                new HistoryRecord(source, position, databaseName, schemaName, ddl, changes);
        for (TableChanges.TableChange change : changes) {
            switch (change.getType()) {
                case CREATE:
                case ALTER:
                    tables.put(
                            change.getId(),
                            new HistoryRecord(tableChangesSerializer.toDocument(change)));
                    break;
                case DROP:
                    tables.remove(change.getId());
                    break;
                default:
                    // impossible
                    throw new RuntimeException(
                            String.format("Unknown change type: %s.", change.getType()));
            }
        }
        listener.onChangeApplied(record);
    }

    @Override
    public void recover(
            Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (HistoryRecord record : tables.values()) {
            TableChange tableChange =
                    JsonTableChangeSerializer.fromDocument(
                            record.document(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public void stop() {
        if (instanceName != null) {
            DebeziumSourceFunction.StateUtils.removeHistory(instanceName);
        }
        listener.stopped();
    }

    @Override
    public boolean exists() {
        return tables != null && !tables.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // do nothing
    }

    @Override
    public boolean storeOnlyMonitoredTables() {
        return storeOnlyMonitoredTablesDdl;
    }

    @Override
    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    /** Determine the {@link FlinkDatabaseSchemaHistory} is compatible with the specified state. */
    public static boolean isCompatible(Collection<HistoryRecord> records) {
        for (HistoryRecord record : records) {
            // Try to deserialize the record
            try {
                TableChange tableChange =
                        JsonTableChangeSerializer.fromDocument(record.document(), false);
                // No table change in the state
                if (tableChange.getId() == null) {
                    return false;
                }
            } catch (Exception e) {
                // Failed to deserialize the table from the state.
                return false;
            }
        }
        return true;
    }
}
