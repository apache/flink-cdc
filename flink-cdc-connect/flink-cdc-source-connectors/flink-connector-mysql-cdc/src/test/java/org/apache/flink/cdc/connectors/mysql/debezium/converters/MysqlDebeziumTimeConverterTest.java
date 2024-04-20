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

package org.apache.flink.cdc.connectors.mysql.debezium.converters;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.debezium.connector.mysql.converters.MysqlDebeziumTimeConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

/** Test for {@link io.debezium.connector.mysql.converters.MysqlDebeziumTimeConverter}. */
public class MysqlDebeziumTimeConverterTest extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "date_convert_test", "mysqluser", "mysqlpw");

    @Test
    public void testReadDateConvertDataStreamSource() throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSourceBuilder<String> builder =
                MySqlSource.<String>builder()
                        .hostname(customDatabase.getHost())
                        .port(customDatabase.getDatabasePort())
                        .databaseList(customDatabase.getDatabaseName())
                        .tableList(customDatabase.getDatabaseName() + ".date_convert_test")
                        .startupOptions(StartupOptions.initial())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .username(customDatabase.getUsername())
                        .password(customDatabase.getPassword())
                        .debeziumProperties(getDebeziumConfigurations());
        builder.deserializer(new JsonDebeziumDeserializationSchema());
        DataStreamSource<String> convertDataStreamSource =
                env.fromSource(
                        builder.build(),
                        WatermarkStrategy.noWatermarks(),
                        "testReadDateConvertDataStreamSource");
        List<String> result = convertDataStreamSource.executeAndCollect(3);
    }

    @Test
    public void testReadDateConvertSQLSource() throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(200L);
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " test_timestamp STRING,"
                                + " test_datetime STRING,"
                                + " test_date STRING,"
                                + " test_time STRING, "
                                + "primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = 'date_convert_test',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'debezium.converters' = 'datetime',"
                                + " 'debezium.datetime.database.type' = 'mysql',"
                                + " 'debezium.datetime.type' = '%s',"
                                + " 'debezium.datetime.format.date' = 'yyyy-MM-dd',"
                                + " 'debezium.datetime.format.time' = 'HH:mm:ss',"
                                + " 'debezium.datetime.format.datetime' = 'yyyy-MM-dd HH:mm:ss',"
                                + " 'debezium.datetime.format.default.value.convert' = 'true'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        "initial",
                        MysqlDebeziumTimeConverter.class.getName());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");
        checkData(tableResult);
    }

    public Properties getDebeziumConfigurations() {
        Properties debeziumProperties = new Properties();
        // set properties
        debeziumProperties.setProperty("converters", "datetime");
        debeziumProperties.setProperty("datetime.database.type", "mysql");
        debeziumProperties.setProperty("datetime.type", MysqlDebeziumTimeConverter.class.getName());
        debeziumProperties.setProperty("datetime.format.date", "yyyy-MM-dd");
        debeziumProperties.setProperty("datetime.format.time", "HH:mm:ss");
        debeziumProperties.setProperty("datetime.format.datetime", "yyyy-MM-dd HH:mm:ss");
        debeziumProperties.setProperty("datetime.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        debeziumProperties.setProperty("datetime.format.default.value.convert", "false");
        // If not set time convert maybe error
        debeziumProperties.setProperty("database.connectionTimeZone", "GMT+8");
        log.info("Supplied debezium properties: {}", debeziumProperties);
        return debeziumProperties;
    }

    private void checkData(TableResult tableResult) {
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[1, 22:23:00, 2023-04-01 14:24:00, 2023-04-01, 14:25:00]",
                    "+I[3, 08:00:00, null, null, 00:01:20]",
                    "+I[2, 08:00:00, null, null, 00:00:00]"
                };

        List<String> expectedSnapshotData = new ArrayList<>(Arrays.asList(snapshotForSingleTable));
        CloseableIterator<Row> collect = tableResult.collect();
        tableResult.getJobClient().get().getJobID();
        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(collect, expectedSnapshotData.size()));
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }
}
