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

import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction.StateUtils;
import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Inspired from {@link io.debezium.relational.history.MemoryDatabaseHistory} but we will store the
 * HistoryRecords in Flink's state for persistence.
 *
 * <p>Note: This is not a clean solution because we depends on a global variable and all the history
 * records will be stored in state (grow infinitely). We may need to come up with a
 * FileSystemDatabaseHistory in the future to store history in HDFS.
 */
public class FlinkDatabaseHistory extends AbstractDatabaseHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    private ConcurrentLinkedQueue<HistoryRecord> records;
    private String instanceName;

    /** Gets the registered HistoryRecords under the given instance name. */
    private ConcurrentLinkedQueue<HistoryRecord> getRegisteredHistoryRecord(String instanceName) {
        Collection<HistoryRecord> historyRecords = StateUtils.retrieveHistory(instanceName);
        return new ConcurrentLinkedQueue<>(historyRecords);
    }

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.records = getRegisteredHistoryRecord(instanceName);

        // register the change into state
        // every change should be visible to the source function
        StateUtils.registerHistory(instanceName, records);
    }

    @Override
    public void stop() {
        super.stop();
        StateUtils.removeHistory(instanceName);
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        this.records.add(record);
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        this.records.forEach(records);
    }

    @Override
    public boolean exists() {
        return !records.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public String toString() {
        return "Flink Database History";
    }

    /** Determine the {@link FlinkDatabaseHistory} is compatible with the specified state. */
    public static boolean isCompatible(Collection<HistoryRecord> records) {
        for (HistoryRecord record : records) {
            // check the source/position/ddl is not null
            if (isNullValue(record.document(), HistoryRecord.Fields.POSITION)
                    || isNullValue(record.document(), HistoryRecord.Fields.SOURCE)
                    || isNullValue(record.document(), HistoryRecord.Fields.DDL_STATEMENTS)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNullValue(Document document, CharSequence field) {
        return document.getField(field).getValue().equals(Value.nullValue());
    }
}
