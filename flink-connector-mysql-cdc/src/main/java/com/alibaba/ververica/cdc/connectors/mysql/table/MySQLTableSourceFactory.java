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

package com.alibaba.ververica.cdc.connectors.mysql.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory for creating configured instance of {@link MySQLTableSource}.
 */
public class MySQLTableSourceFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = "mysql-cdc";

	private static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
		.stringType()
		.noDefaultValue()
		.withDescription("IP address or hostname of the MySQL database server.");

	private static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
		.intType()
		.defaultValue(3306)
		.withDescription("Integer port number of the MySQL database server.");

	private static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of the MySQL database to use when connecting to the MySQL database server.");

	private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("Password to use when connecting to the MySQL database server.");

	private static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Database name of the MySQL server to monitor.");

	private static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Table name of the MySQL database to monitor.");

	private static final ConfigOption<Integer> SERVER_ID = ConfigOptions.key("server-id")
		.intType()
		.noDefaultValue()
		.withDescription("A numeric ID of this database client, which must be unique across all " +
			"currently-running database processes in the MySQL cluster. This connector joins the " +
			"MySQL database cluster as another server (with this unique ID) so it can read the binlog. " +
			"By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();

		final ReadableConfig config = helper.getOptions();
		String hostname = config.get(HOSTNAME);
		String username = config.get(USERNAME);
		String password = config.get(PASSWORD);
		String databaseName = config.get(DATABASE_NAME);
		String tableName = config.get(TABLE_NAME);
		int port = config.get(PORT);
		Integer serverId = config.getOptional(SERVER_ID).orElse(null);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		return new MySQLTableSource(
			physicalSchema,
			port,
			hostname,
			databaseName,
			tableName,
			username,
			password,
			serverId
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOSTNAME);
		options.add(USERNAME);
		options.add(PASSWORD);
		options.add(DATABASE_NAME);
		options.add(TABLE_NAME);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PORT);
		options.add(SERVER_ID);
		return options;
	}
}
