/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */

package org.apache.flink.cdc.debezium.internal;

import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.relational.history.TableChanges.TableChange;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.registerHistory;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.removeHistory;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.retrieveHistory;

/**
 * Stores only the latest schema of monitored tables. Updated for Debezium 3.x API compatibility.
 */
public class FlinkDatabaseSchemaHistory implements SchemaHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    private final FlinkJsonTableChangeSerializer tableChangesSerializer =
            new FlinkJsonTableChangeSerializer();

    private ConcurrentMap<TableId, SchemaRecord> latestTables;
    private String instanceName;
    private SchemaHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;
    private boolean useCatalogBeforeSchema;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.listener = listener;
        // Use STORE_ONLY_CAPTURED_TABLES_DDL (renamed in Debezium 3.x)
        this.storeOnlyMonitoredTablesDdl =
                config.getBoolean(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;

        this.latestTables = new ConcurrentHashMap<>();
        for (SchemaRecord schemaRecord : retrieveHistory(instanceName)) {
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            schemaRecord.toDocument(), useCatalogBeforeSchema);
            latestTables.put(tableChange.getId(), schemaRecord);
        }
        registerHistory(instanceName, latestTables.values());
    }

    @Override
    public void start() {
        if (listener != null) {
            listener.started();
        }
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws SchemaHistoryException {
        // no-op: FlinkDatabaseSchemaHistory uses TableChanges-based record() only
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes,
            Instant timestamp)
            throws SchemaHistoryException {
        for (TableChanges.TableChange change : changes) {
            switch (change.getType()) {
                case CREATE:
                case ALTER:
                    latestTables.put(
                            change.getId(),
                            new SchemaRecord(tableChangesSerializer.toDocument(change)));
                    break;
                case DROP:
                    latestTables.remove(change.getId());
                    break;
                default:
                    throw new RuntimeException(
                            String.format("Unknown change type: %s.", change.getType()));
            }
        }
        if (listener != null) {
            listener.onChangeApplied(
                    new HistoryRecord(
                            source, position, databaseName, schemaName, ddl, changes, timestamp));
        }
    }

    @Override
    public void recover(
            Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        if (listener != null) {
            listener.recoveryStarted();
        }
        for (SchemaRecord record : latestTables.values()) {
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            record.getTableChangeDoc(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        if (listener != null) {
            listener.recoveryStopped();
        }
    }

    @Override
    public void recover(
            Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        if (listener != null) {
            listener.recoveryStarted();
        }
        for (SchemaRecord record : latestTables.values()) {
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            record.getTableChangeDoc(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        if (listener != null) {
            listener.recoveryStopped();
        }
    }

    @Override
    public void stop() {
        if (instanceName != null) {
            removeHistory(instanceName);
        }
        if (listener != null) {
            listener.stopped();
        }
    }

    @Override
    public boolean exists() {
        return latestTables != null && !latestTables.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // no-op
    }

    public static boolean isCompatible(Collection<SchemaRecord> records) {
        for (SchemaRecord record : records) {
            if (!record.isTableChangeRecord()) {
                return false;
            } else {
                break;
            }
        }
        return true;
    }
}
