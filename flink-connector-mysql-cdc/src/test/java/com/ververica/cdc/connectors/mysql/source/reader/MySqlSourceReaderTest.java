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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.RecordsFormatter;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link MySqlSourceReader}. */
public class MySqlSourceReaderTest extends MySqlParallelSourceTestBase {

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @Test
    public void testBinlogReadFailoverCrossTransaction() throws Exception {
        customerDatabase.createAndInitialize();
        final MySqlSourceConfig configuration = getConfig(new String[] {"customers"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        MySqlSplit binlogSplit = createBinlogSplit(configuration);

        MySqlSourceReader<SourceRecord> reader = createReader(configuration);
        reader.start();
        reader.addSplits(Arrays.asList(binlogSplit));

        // step-1: make 6 change events in one MySQL transaction
        TableId tableId = binlogSplit.getTableSchemas().keySet().iterator().next();
        makeBinlogEventsInOneTransaction(configuration, tableId.toString());

        // step-2: fetch the first 2 records belong to the MySQL transaction
        String[] expectedRecords =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]"
                };
        // the 2 records are produced by 1 operations
        List<String> actualRecords = consumeRecords(reader, dataType, 1);
        assertEqualsInOrder(Arrays.asList(expectedRecords), actualRecords);
        List<MySqlSplit> splitsState = reader.snapshotState(1L);
        // check the binlog split state
        assertEquals(1, splitsState.size());
        reader.close();

        // step-3: mock failover from a restored state
        MySqlSourceReader<SourceRecord> restartReader = createReader(configuration);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // step-4: fetch the rest 4 records belong to the MySQL transaction
        String[] expectedRestRecords =
                new String[] {
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]"
                };
        // the 4 records are produced by 3 operations
        List<String> restRecords = consumeRecords(restartReader, dataType, 3);
        assertEqualsInOrder(Arrays.asList(expectedRestRecords), restRecords);
        restartReader.close();
    }

    private MySqlSourceReader<SourceRecord> createReader(MySqlSourceConfig configuration) {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final SourceReaderContext readerContext = new TestingReaderContext();
        final MySqlRecordEmitter<SourceRecord> recordEmitter =
                new MySqlRecordEmitter<>(
                        new ForwardDeserializeSchema(),
                        new MySqlSourceReaderMetrics(readerContext.metricGroup()),
                        configuration.isIncludeSchemaChanges());
        return new MySqlSourceReader<>(
                elementsQueue,
                () -> createSplitReader(configuration),
                recordEmitter,
                readerContext.getConfiguration(),
                readerContext);
    }

    private MySqlSplitReader createSplitReader(MySqlSourceConfig configuration) {
        return new MySqlSplitReader(configuration, 0);
    }

    private void makeBinlogEventsInOneTransaction(MySqlSourceConfig sourceConfig, String tableId)
            throws SQLException {
        JdbcConnection connection =
                DebeziumUtils.openMySqlConnection(sourceConfig.getDbzConfiguration());
        // make 6 binlog events by 4 operations
        connection.setAutoCommit(false);
        connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
        connection.commit();
        connection.close();
    }

    private MySqlSplit createBinlogSplit(MySqlSourceConfig sourceConfig) {
        MySqlBinlogSplitAssigner binlogSplitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        binlogSplitAssigner.open();
        return binlogSplitAssigner.getNext().get();
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .includeSchemaChanges(false)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .splitSize(10)
                .fetchSize(2)
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }

    private List<String> consumeRecords(
            MySqlSourceReader<SourceRecord> sourceReader, DataType recordType, int changeEventNum)
            throws Exception {
        // Poll all the n records of the single split.
        final SimpleReaderOutput output = new SimpleReaderOutput();
        while (output.getResults().size() < changeEventNum) {
            sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    private static class SimpleReaderOutput implements ReaderOutput<SourceRecord> {

        private final List<SourceRecord> results = new ArrayList<>();

        @Override
        public void collect(SourceRecord record) {
            results.add(record);
        }

        public List<SourceRecord> getResults() {
            return results;
        }

        @Override
        public void collect(SourceRecord record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public SourceOutput<SourceRecord> createOutputForSplit(java.lang.String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(java.lang.String splitId) {}
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }
}
