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

package com.alibaba.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLTestBase;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Tests class for {@link MySQLSource}.
 */
public class MySQLSourceITCase extends MySQLTestBase {

    private UniqueDatabase customDatabase =
        new UniqueDatabase(MYSQL_CONTAINER, "custom", "mysqluser", "mysqlpw");

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testReadMySQLTable(1, new String[]{"customers"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testReadMySQLTable(4, new String[]{"customers"});
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testReadMySQLTable(1, new String[]{"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testReadMySQLTable(4, new String[]{"customers", "customers_1"});
    }

    public void testReadMySQLTable(int parallelism, String[] captureTables) throws Exception {
        customDatabase.createAndInitialize();
        final DataType dataType =
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("address", DataTypes.STRING()),
                DataTypes.FIELD("phone_number", DataTypes.STRING()));
        final RowType pkType =
            (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        final DebeziumDeserializationSchema<Row> deserializer = getDeserializationSchema(dataType);

        final Configuration configuration = getConfig();
        List<String> captureTableIds = Arrays.stream(captureTables)
            .map(tableName -> customDatabase.getDatabaseName() + "." + tableName)
            .collect(Collectors.toList());
        configuration.setString("table.whitelist", String.join(",", captureTableIds));

        MySQLSource<Row> source = new MySQLSource<>(pkType, deserializer, configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> stream =
            env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-parallel-source")
                // set source readers
                .setParallelism(parallelism);
        final CloseableIterator<Row> iterator = stream.executeAndCollect();

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
            new String[]{
                "+I[101, user_1, Shanghai, 123567891234]",
                "+I[102, user_2, Shanghai, 123567891234]",
                "+I[103, user_3, Shanghai, 123567891234]",
                "+I[109, user_4, Shanghai, 123567891234]",
                "+I[110, user_5, Shanghai, 123567891234]",
                "+I[111, user_6, Shanghai, 123567891234]",
                "+I[118, user_7, Shanghai, 123567891234]",
                "+I[121, user_8, Shanghai, 123567891234]",
                "+I[123, user_9, Shanghai, 123567891234]",
                "+I[1009, user_10, Shanghai, 123567891234]",
                "+I[1010, user_11, Shanghai, 123567891234]",
                "+I[1011, user_12, Shanghai, 123567891234]",
                "+I[1012, user_13, Shanghai, 123567891234]",
                "+I[1013, user_14, Shanghai, 123567891234]",
                "+I[1014, user_15, Shanghai, 123567891234]",
                "+I[1015, user_16, Shanghai, 123567891234]",
                "+I[1016, user_17, Shanghai, 123567891234]",
                "+I[1017, user_18, Shanghai, 123567891234]",
                "+I[1018, user_19, Shanghai, 123567891234]",
                "+I[1019, user_20, Shanghai, 123567891234]",
                "+I[2000, user_21, Shanghai, 123567891234]"
            };

        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }
        String[] expectedSnapshot = expectedSnapshotData.toArray(new String[0]);
        assertThat(fetchRow(iterator, expectedSnapshot.length), containsInAnyOrder(expectedSnapshot));

        // second step: check the binlog data
        MySqlConnection connection = StatefulTaskContext.getConnection(configuration);

        for (String tableId : captureTableIds) {
            makeBinlogEvents(connection, tableId);
        }

        String[] binlogForSingleTable =
            new String[]{
                "-U[103, user_3, Shanghai, 123567891234]",
                "+U[103, user_3, Hangzhou, 123567891234]",
                "-D[102, user_2, Shanghai, 123567891234]",
                "+I[102, user_2, Shanghai, 123567891234]",
                "-U[103, user_3, Hangzhou, 123567891234]",
                "+U[103, user_3, Shanghai, 123567891234]",
                "-U[1010, user_11, Shanghai, 123567891234]",
                "+U[1010, user_11, Hangzhou, 123567891234]",
                "+I[2001, user_22, Shanghai, 123567891234]",
                "+I[2002, user_23, Shanghai, 123567891234]",
                "+I[2003, user_24, Shanghai, 123567891234]"
            };
        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureTables.length; i++) {
            expectedBinlogData.addAll(Arrays.asList(binlogForSingleTable));
        }
        String[] expectedBinlog = expectedBinlogData.toArray(new String[0]);
        assertThat(fetchRow(iterator, expectedBinlog.length), containsInAnyOrder(expectedBinlog));
        iterator.close();
    }

    private List<String> fetchRow(CloseableIterator<Row> iterator, int fetchSize) throws Exception {
        List<String> rows = new ArrayList<>();
        while (fetchSize > 0 && iterator.hasNext()) {
            Row row = iterator.next();
            rows.add(row.toString());
            fetchSize--;
        }
        return rows;
    }

    private void makeBinlogEvents(JdbcConnection connection, String tableId)
        throws SQLException {
        // make binlog events for the first split
        try {
            connection.setAutoCommit(false);
            connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();

            // make binlog events for split-1
            connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                "INSERT INTO "
                    + tableId
                    + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                    + " (2002, 'user_23','Shanghai','123567891234'),"
                    + "(2003, 'user_24','Shanghai','123567891234')");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private DebeziumDeserializationSchema<Row> getDeserializationSchema(DataType dataType) {
        final DebeziumDeserializationSchema<RowData> deserializationSchema =
                new RowDataDebeziumDeserializeSchema(
                        (RowType) dataType.getLogicalType(),
                        InternalTypeInfo.of((RowType) dataType.getLogicalType()),
                        ((rowData, rowKind) -> {}),
                        ZoneId.of("UTC"));
        final RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        return new RowDebeziumDeserializationSchema(
                deserializationSchema,
                rowRowConverter,
                (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(dataType));
    }

    /**
     * A {@link DebeziumDeserializationSchema} that deserializes {@link SourceRecord}
     * to {@link Row}.
     *
     * <p>This schema is used in test to format the result to be readable.
     */
    private static class RowDebeziumDeserializationSchema
            implements DebeziumDeserializationSchema<Row> {

        private static final long serialVersionUID = 1L;
        private final DebeziumDeserializationSchema<RowData> deserializationSchema;
        private final RowRowConverter rowRowConverter;

        private final TypeInformation<Row> outputType;

        public RowDebeziumDeserializationSchema(
                DebeziumDeserializationSchema<RowData> deserializationSchema,
                RowRowConverter rowRowConverter,
                TypeInformation<Row> outputType) {
            this.deserializationSchema = deserializationSchema;
            this.rowRowConverter = rowRowConverter;
            this.outputType = outputType;
            this.rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {

            deserializationSchema.deserialize(
                    record,
                    new Collector<RowData>() {
                        @Override
                        public void collect(RowData record) {
                            out.collect(rowRowConverter.toExternal(record));
                        }

                        @Override
                        public void close() {}
                    });
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return outputType;
        }
    }

    private Configuration getConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.whitelist", customDatabase.getDatabaseName());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id.range", "1001,1004");
        properties.put("scan.split.size", "4");
        properties.put("scan.fetch.size", "2");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", DATABASE_HISTORY_INSTANCE_NAME);
        return Configuration.fromMap(properties);
    }
}
