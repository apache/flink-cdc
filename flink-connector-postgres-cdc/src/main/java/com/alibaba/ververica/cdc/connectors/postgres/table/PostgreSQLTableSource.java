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

package com.alibaba.ververica.cdc.connectors.postgres.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.config.Configuration;
import io.debezium.heartbeat.Heartbeat;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a PostgreSQL source from a logical
 * description.
 */
public class PostgreSQLTableSource implements ScanTableSource {

	private final TableSchema physicalSchema;
	private final int port;
	private final String hostname;
	private final String database;
	private final String schemaName;
	private final String tableName;
	private final String username;
	private final String password;
	private final String pluginName;
	private final Properties dbzProperties;

	public PostgreSQLTableSource(
			TableSchema physicalSchema,
			int port,
			String hostname,
			String database,
			String schemaName,
			String tableName,
			String username,
			String password,
			String pluginName,
			Properties dbzProperties) {
		this.physicalSchema = physicalSchema;
		this.port = port;
		this.hostname = checkNotNull(hostname);
		this.database = checkNotNull(database);
		this.schemaName = checkNotNull(schemaName);
		this.tableName = checkNotNull(tableName);
		this.username = checkNotNull(username);
		this.password = checkNotNull(password);
		this.pluginName = checkNotNull(pluginName);
		this.dbzProperties = dbzProperties;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.UPDATE_BEFORE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.addContainedKind(RowKind.DELETE)
			.build();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
		RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
		TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(physicalSchema.toRowDataType());
		Configuration dbzConfig = Configuration.from(dbzProperties);
		String dbzHeartbeatPrefix = dbzConfig.getString(Heartbeat.HEARTBEAT_TOPICS_PREFIX);
		DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
			rowType,
			typeInfo,
			new PostgresValueValidator(schemaName, tableName, dbzHeartbeatPrefix),
			ZoneId.of("UTC"));
		DebeziumSourceFunction<RowData> sourceFunction = PostgreSQLSource.<RowData>builder()
			.hostname(hostname)
			.port(port)
			.database(database)
			.schemaList(schemaName)
			.tableList(schemaName + "." + tableName)
			.username(username)
			.password(password)
			.decodingPluginName(pluginName)
			.debeziumProperties(dbzProperties)
			.deserializer(deserializer)
			.build();
		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new PostgreSQLTableSource(
			physicalSchema,
			port,
			hostname,
			database,
			schemaName,
			tableName,
			username,
			password,
			pluginName,
			dbzProperties);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PostgreSQLTableSource that = (PostgreSQLTableSource) o;
		return port == that.port &&
			Objects.equals(physicalSchema, that.physicalSchema) &&
			Objects.equals(hostname, that.hostname) &&
			Objects.equals(database, that.database) &&
			Objects.equals(schemaName, that.schemaName) &&
			Objects.equals(tableName, that.tableName) &&
			Objects.equals(username, that.username) &&
			Objects.equals(password, that.password) &&
			Objects.equals(dbzProperties, that.dbzProperties);
	}

	@Override
	public int hashCode() {
		return Objects.hash(physicalSchema, port, hostname, database, schemaName, tableName, username, password, dbzProperties);
	}

	@Override
	public String asSummaryString() {
		return "PostgreSQL-CDC";
	}
}
