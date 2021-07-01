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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLTestBase;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySQLSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isWatermarkEvent;
import static org.junit.Assert.assertEquals;

/** Tests for {@link SnapshotSplitReader}. */
public class SnapshotSplitReaderTest extends MySQLTestBase {

    private static final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "custom", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customDatabase.createAndInitialize();
    }

    @Test
    public void testReadSingleSnapshotSplit() throws Exception {
        Configuration configuration = getConfig();
        RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<MySQLSplit> mySQLSplits = getMySQLSplits(configuration, pkType);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]"
                };
        List<String> actual = readTableSnapshotSplits(mySQLSplits, configuration, 1);
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    @Test
    public void testReadAllSnapshotSplits() throws Exception {
        Configuration configuration = getConfig();
        RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<MySQLSplit> mySQLSplits = getMySQLSplits(configuration, pkType);

        String[] expected =
                new String[] {
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
        List<String> actual =
                readTableSnapshotSplits(mySQLSplits, configuration, mySQLSplits.size());
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    private List<String> readTableSnapshotSplits(
            List<MySQLSplit> mySQLSplits, Configuration configuration, int scanSplitsNum)
            throws Exception {
        final BinaryLogClient binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        final MySqlConnection mySqlConnection = StatefulTaskContext.getConnection(configuration);
        StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(configuration, binaryLogClient, mySqlConnection);
        SnapshotSplitReader snapshotSplitReader = new SnapshotSplitReader(statefulTaskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            MySQLSplit sqlSplit = mySQLSplits.get(i);
            if (snapshotSplitReader.isIdle()) {
                snapshotSplitReader.submitSplit(sqlSplit);
            }
            Iterator<SourceRecord> res;
            while ((res = snapshotSplitReader.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecord sourceRecord = res.next();
                    result.add(sourceRecord);
                }
            }
        }

        return formatResult(result);
    }

    private List<String> formatResult(List<SourceRecord> records) {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(dataType);
        final DebeziumDeserializationSchema<RowData> deserializationSchema =
                new RowDataDebeziumDeserializeSchema(
                        rowType, typeInfo, ((rowData, rowKind) -> {}), ZoneId.of("UTC"));
        SimpleCollector collector = new SimpleCollector();
        RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        records.stream()
                // filter signal event
                .map(
                        record -> {
                            System.out.println(record);
                            return record;
                        })
                .filter(r -> !isWatermarkEvent(r))
                .forEach(
                        r -> {
                            try {
                                deserializationSchema.deserialize(r, collector);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
        return collector.list.stream()
                .map(rowRowConverter::toExternal)
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<MySQLSplit> getMySQLSplits(Configuration configuration, RowType pkType) {
        MySQLSnapshotSplitAssigner assigner =
                new MySQLSnapshotSplitAssigner(
                        configuration, pkType, new ArrayList<>(), new ArrayList<>());
        assigner.open();
        List<MySQLSplit> mySQLSplitList = new ArrayList<>();
        while (true) {
            Optional<MySQLSplit> mySQLSplit = assigner.getNext(null);
            if (mySQLSplit.isPresent()) {
                mySQLSplitList.add(mySQLSplit.get());
            } else {
                break;
            }
        }
        assigner.close();
        return mySQLSplitList;
    }

    private Configuration getConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id-range", "1001, 1002");
        properties.put("scan.split.size", "10");
        properties.put("scan.fetch.size", "2");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", DATABASE_HISTORY_INSTANCE_NAME);
        return Configuration.fromMap(properties);
    }

    static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
