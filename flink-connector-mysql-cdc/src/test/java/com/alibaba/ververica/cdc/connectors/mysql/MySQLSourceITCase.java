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

package com.alibaba.ververica.cdc.connectors.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.alibaba.ververica.cdc.connectors.mysql.utils.UniqueDatabase;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests for {@link MySQLSource}.
 */
@Ignore
public class MySQLSourceITCase extends MySQLTestBase {

	private final UniqueDatabase inventoryDatabase = new UniqueDatabase(
		MYSQL_CONTAINER,
		"inventory",
		"mysqluser",
		"mysqlpw");

	@Test
	public void testConsumingAllEvents() throws Exception {
		inventoryDatabase.createAndInitialize();
		SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
			.hostname(MYSQL_CONTAINER.getHost())
			.port(MYSQL_CONTAINER.getDatabasePort())
			.databaseList(inventoryDatabase.getDatabaseName()) // monitor all tables under inventory database
			.username(inventoryDatabase.getUsername())
			.password(inventoryDatabase.getPassword())
			.deserializer(new StringDebeziumDeserializationSchema())
			.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(sourceFunction).print().setParallelism(1);

		env.execute("Print MySQL Snapshot + Binlog");
	}
}
