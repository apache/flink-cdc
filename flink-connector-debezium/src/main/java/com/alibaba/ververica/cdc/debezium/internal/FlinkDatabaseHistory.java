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

import org.apache.flink.runtime.state.FunctionSnapshotContext;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Inspired from {@link io.debezium.relational.history.MemoryDatabaseHistory} but we will store
 * the HistoryRecords in Flink's state for persistence.
 *
 * <p>Note: This is not a clean solution because we depends on a global variable and all the history
 * records will be stored in state (grow infinitely). We may need to come up with a
 * FileSystemDatabaseHistory in the future to store history in HDFS.
 */
public class FlinkDatabaseHistory extends AbstractDatabaseHistory {

	public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

	/**
	 * We will synchronize the records into Flink's state during snapshot.
	 * We have to use a global variable to communicate with Flink's source function,
	 * because Debezium will construct the instance of {@link DatabaseHistory} itself.
	 * Maybe we can improve this in the future.
	 *
	 * <p>NOTE: we just use Flink's state as a durable persistent storage as a replacement of
	 * {@link FileDatabaseHistory} and {@link KafkaDatabaseHistory}. It doesn't need to guarantee
	 * the exactly-once semantic for the history records. The history records shouldn't be super
	 * large, because we only monitor the schema changes for one single table.
	 *
	 * @see com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction#snapshotState(FunctionSnapshotContext)
	 */
	public static final Map<String, ConcurrentLinkedQueue<HistoryRecord>> ALL_RECORDS = new HashMap<>();

	private ConcurrentLinkedQueue<HistoryRecord> records;
	private String instanceName;
	private boolean databaseexists;

	/**
	 * Registers the given HistoryRecords into global variable under the given instance name,
	 * in order to be accessed by instance of {@link FlinkDatabaseHistory}.
	 */
	public static void registerHistoryRecords(String instanceName, ConcurrentLinkedQueue<HistoryRecord> historyRecords) {
		synchronized (FlinkDatabaseHistory.ALL_RECORDS) {
			FlinkDatabaseHistory.ALL_RECORDS.put(instanceName, historyRecords);
		}
	}

	/**
	 * Registers an empty HistoryRecords into global variable under the given instance name,
	 * in order to be accessed by instance of {@link FlinkDatabaseHistory}.
	 */
	public static void registerEmptyHistoryRecord(String instanceName) {
		registerHistoryRecords(instanceName, new ConcurrentLinkedQueue<>());
	}

	/**
	 * Gets the registered HistoryRecords under the given instance name.
	 */
	public static ConcurrentLinkedQueue<HistoryRecord> getRegisteredHistoryRecord(String instanceName) {
		synchronized (ALL_RECORDS) {
			if (ALL_RECORDS.containsKey(instanceName)) {
				return ALL_RECORDS.get(instanceName);
			}
		}
		return null;
	}

	@Override
	public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
		super.configure(config, comparator, listener, useCatalogBeforeSchema);
		this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
		this.records = getRegisteredHistoryRecord(instanceName);
		if (records == null) {
			throw new IllegalStateException(
				String.format("Couldn't find engine instance %s in the global records.", instanceName));
		}
		this.databaseexists = config.getBoolean("database.history.exists", true);
	}

	@Override
	public void stop() {
		super.stop();
		if (instanceName != null) {
			synchronized (ALL_RECORDS) {
				// clear memory
				ALL_RECORDS.remove(instanceName);
			}
		}
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
		return databaseexists;
	}

	@Override
	public boolean storageExists() {
		return true;
	}

	@Override
	public String toString() {
		return "Flink Database History";
	}
}
