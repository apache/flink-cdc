package com.ververica.cdc.connectors.mysql.source;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * IT cases for {@link MySqlSource} to test the gtids offset is correctly stored and restored.
 */
public class MySqlGtidsOffsetITCase extends MySqlSourceTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(MySqlGtidsOffsetITCase.class);

    private final UniqueDatabase gtidsDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "gtids_test", "mysqluser", "mysqlpw");

    private final String tableId = gtidsDatabase.getDatabaseName() + ".products";

    private final String historyPurgedGtids = "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543," +
            "85ebef13-1e2d-11e7-a18e-080027d89544:1-27336452";

    private static boolean isDatabaseInitialized = false;
    
    private static final long waitFlinkBootTime = 5000;

    @Before
    public void initDatabase() throws Exception {
        if (!isDatabaseInitialized) {
            // Initialize the database for the first time
            gtidsDatabase.createAndInitialize();
            isDatabaseInitialized = true;
        } else {
            // For non-initial database initialization, it is necessary to reset the GTID to avoid interference from
            // previous tests and create some events to generate new GTIDs for obtaining source IDs.
            gtidsDatabase.createAndInitialize();
            Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
            resetMaster(jdbcConnection);
            addData(jdbcConnection, 1);
            truncate(jdbcConnection);
        }
    }

    @Test
    public void doSingleGtidsInitialTest() throws Exception {
        // Set parameters and expected data for test cases
        StartupOptions startupOptions = StartupOptions.initial();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9"; //

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5); //
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsInitialTest() throws Exception {
        // Set parameters and expected data for test cases.
        StartupOptions startupOptions = StartupOptions.initial();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsLatestTest() throws Exception {
        // Set parameters and expected data for test cases
        StartupOptions startupOptions = StartupOptions.latest();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 5;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsLatestTest() throws Exception {
        // Set parameters and expected data for test cases.
        StartupOptions startupOptions = StartupOptions.latest();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 5;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsEarliestTest() throws Exception {
        // Set parameters and expected data for test cases.
        StartupOptions startupOptions = StartupOptions.earliest();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsEarliestTest() throws Exception {
        // Set parameters and expected data for test cases.
        StartupOptions startupOptions = StartupOptions.earliest();
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsOffsetGtidByBeginningTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        StartupOptions startupOptions = StartupOptions.specificOffset(initSourceId + ":1-3");
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 7;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsOffsetGtidByMiddleTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        StartupOptions startupOptions = StartupOptions.specificOffset(initSourceId + ":3-4");
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 8;
        String expectedTransactionId = "1-2:5-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsOffsetGtidByBeginningIncorrectSettingTest() throws Exception {
        try {
            // Set parameters and expected data for test cases.
            Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
            String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
            StartupOptions startupOptions = StartupOptions.specificOffset(initSourceId + ":1-3");
            int actuallyAddDataCount = 5;
            int actuallyFetchDataCount = 7;
            String expectedTransactionId = "1-9";

            // Predefined Gtids for the database
            resetMaster(gtidsDatabase.getJdbcConnection());
            setPurgedGitds(jdbcConnection, historyPurgedGtids);
            addData(jdbcConnection, 5);
            MySqlSource<String> mySqlSource = getSource(startupOptions);

            // Run Flink CDC, insert data, and finally validate assertions
            executeCdcAndAssert(
                    jdbcConnection,
                    mySqlSource,
                    actuallyAddDataCount,
                    actuallyFetchDataCount,
                    initSourceId,
                    historyPurgedGtids,
                    expectedTransactionId
            );
        } catch (Exception e) {
            assertEquals("Failed to fetch next result", e.getMessage());
        }
    }

    @Test
    public void doMultipleGtidsOffsetGtidByBeginningCorrectSettingTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        StartupOptions startupOptions = StartupOptions.specificOffset(historyPurgedGtids + "," + initSourceId + ":1-3");
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 7;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsOffsetGtidByMiddleIncorrectSettingTest() {
        try {
            // Set parameters and expected data for test cases.
            Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
            String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
            StartupOptions startupOptions = StartupOptions.specificOffset(initSourceId + ":3-4");
            int actuallyAddDataCount = 5;
            int actuallyFetchDataCount = 8;
            String expectedTransactionId = "1-9";

            // Predefined Gtids for the database
            resetMaster(gtidsDatabase.getJdbcConnection());
            setPurgedGitds(jdbcConnection, historyPurgedGtids);
            addData(jdbcConnection, 5);
            MySqlSource<String> mySqlSource = getSource(startupOptions);

            // Run Flink CDC, insert data, and finally validate assertions
            executeCdcAndAssert(
                    jdbcConnection,
                    mySqlSource,
                    actuallyAddDataCount,
                    actuallyFetchDataCount,
                    initSourceId,
                    historyPurgedGtids,
                    expectedTransactionId
            );
        } catch (Exception e) {
            // The exception is expected, because the offset is not in the history gtid set
            assertEquals("Failed to fetch next result", e.getMessage());
        }
    }

    @Test
    public void doMultipleGtidsOffsetGtidByMiddleCorrectSettingTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        StartupOptions startupOptions = StartupOptions.specificOffset(historyPurgedGtids + "," + initSourceId + ":3-4");
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 8;
        String expectedTransactionId = "1-2:5-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsOffsetBinlogFileByBeginningTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        Tuple2<String, Long> binlogFileAndPos = getBinlogFileAndPos(jdbcConnection);
        StartupOptions startupOptions = StartupOptions.specificOffset(binlogFileAndPos.f0, binlogFileAndPos.f1);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsOffsetBinlogFileByMiddleTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 7;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 3);
        Tuple2<String, Long> binlogFileAndPos = getBinlogFileAndPos(jdbcConnection);
        StartupOptions startupOptions = StartupOptions.specificOffset(binlogFileAndPos.f0, binlogFileAndPos.f1);
        addData(jdbcConnection, 2);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsOffsetBinlogFileByBeginningTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        Tuple2<String, Long> binlogFileAndPos = getBinlogFileAndPos(jdbcConnection);
        StartupOptions startupOptions = StartupOptions.specificOffset(binlogFileAndPos.f0, binlogFileAndPos.f1);
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsOffsetBinlogFileByMiddleTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 7;
        String expectedTransactionId = "1-9";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 3);
        Tuple2<String, Long> binlogFileAndPos = getBinlogFileAndPos(jdbcConnection);
        StartupOptions startupOptions = StartupOptions.specificOffset(binlogFileAndPos.f0, binlogFileAndPos.f1);
        addData(jdbcConnection, 2);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    @Test
    public void doSingleGtidsTimestampTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-14";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        addData(jdbcConnection, 5);
        Thread.sleep(1000);
        StartupOptions startupOptions = StartupOptions.timestamp(System.currentTimeMillis());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                null,
                expectedTransactionId
        );
    }

    @Test
    public void doMultipleGtidsTimestampTest() throws Exception {
        // Set parameters and expected data for test cases.
        Connection jdbcConnection = gtidsDatabase.getJdbcConnection();
        String initSourceId = getSourceId(getExecutedGtids(jdbcConnection));
        int actuallyAddDataCount = 5;
        int actuallyFetchDataCount = 10;
        String expectedTransactionId = "1-14";

        // Predefined Gtids for the database
        resetMaster(gtidsDatabase.getJdbcConnection());
        setPurgedGitds(jdbcConnection, historyPurgedGtids);
        addData(jdbcConnection, 5);
        Thread.sleep(1000);
        StartupOptions startupOptions = StartupOptions.timestamp(System.currentTimeMillis());
        addData(jdbcConnection, 5);
        MySqlSource<String> mySqlSource = getSource(startupOptions);

        // Run Flink CDC, insert data, and finally validate assertions
        executeCdcAndAssert(
                jdbcConnection,
                mySqlSource,
                actuallyAddDataCount,
                actuallyFetchDataCount,
                initSourceId,
                historyPurgedGtids,
                expectedTransactionId
        );
    }

    private String getSourceId(String gtids) {
        String[] split = gtids.split(":");
        return split[0];
    }

    private void resetMaster(Connection jdbcConnection) throws Exception {
        jdbcConnection.createStatement().execute("RESET MASTER");
    }

    private void setPurgedGitds(Connection jdbcConnection, String gtids) throws Exception {
        jdbcConnection.createStatement().execute(String.format("SET @@GLOBAL.gtid_purged='%s'", gtids));
    }

    private String getExecutedGtids(Connection jdbcConnection) throws Exception {
        ResultSet resultSet = jdbcConnection.createStatement().executeQuery("SHOW MASTER STATUS");
        if (resultSet.next()) {
            return resultSet.getString("Executed_Gtid_Set");
        }
        throw new Exception("Can not got ExecutedGtids");
    }

    private Tuple2<String, Long> getBinlogFileAndPos(Connection jdbcConnection) throws Exception {
        ResultSet resultSet = jdbcConnection.createStatement().executeQuery("SHOW MASTER STATUS");
        if (resultSet.next()) {
            Tuple2<String, Long> BinlogFilePos = new Tuple2<>();
            BinlogFilePos.f0 = (resultSet.getString("File"));
            BinlogFilePos.f1 = (resultSet.getLong("Position"));
            return BinlogFilePos;
        }
        throw new Exception("Can not got ExecutedGtids");
    }

    private String getPurgedGtids(Connection jdbcConnection) throws Exception {
        ResultSet resultSet = jdbcConnection.createStatement().executeQuery("SELECT @@global.gtid_purged");
        if (resultSet.next()) {
            return resultSet.getString("@@global.gtid_purged");
        }
        throw new Exception("Can not got PurgedGtids");
    }

    private void addData(Connection jdbcConnection, int dataCnt) throws Exception {
        for (int i = 0; i < dataCnt; i++) {
            jdbcConnection.createStatement().execute("INSERT INTO products VALUES (default,\"scooter\",\"Small 2-wheel scooter\", 3.14);");
        }
    }

    private void truncate(Connection jdbcConnection) throws Exception {
        jdbcConnection.createStatement().execute("TRUNCATE TABLE products;");
    }

    private StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        return env;
    }

    private RowDataDebeziumDeserializeSchema getProductsTableDeserializer() {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(TypeConversions.fromDataToLogicalType(dataType));
        return RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType((RowType) dataType.getLogicalType())
                        .setResultTypeInfo(typeInfo)
                        .build();
    }

    private MySqlSource<String> getSource(StartupOptions startupOptions) {
        return MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(gtidsDatabase.getDatabaseName())
                        .serverTimeZone("UTC")
                        .tableList(tableId)
                        .username(gtidsDatabase.getUsername())
                        .password(gtidsDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new GtidsDebeziumDeserializationSchema())
                        .startupOptions(startupOptions)
                        .build();
    }

    private static List<String> fetchRowData(Iterator<String> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            String row = iter.next();
            rows.add(row);
            size--;
        }
        return (rows);
    }

    private void executeCdcAndAssert(Connection jdbcConnection, MySqlSource<String> mySqlSource, int addSize, int fetchSize, String initSourceId, String historyGtids, String expectedTransactionId) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> sourceStream =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "selfSource");
        try (CloseableIterator<String> iterator = sourceStream.executeAndCollect()) {
            Thread.sleep(waitFlinkBootTime);
            addData(jdbcConnection, addSize);
            List<String> rows = fetchRowData(iterator, fetchSize);
            LOG.info("expectedFetchSize is {}, actualFetchSize is {}", fetchSize, rows.size());
            assertEquals(fetchSize, rows.size());

            String lastGtids = (rows.size() > 0) ? rows.get(rows.size()-1) : "";
            GtidSet lastGtidSet = new GtidSet(lastGtids);
            GtidSet expectedGtidSet = getExpectedGtidSet(initSourceId, historyGtids, expectedTransactionId);
            LOG.info("expectedGtidSet is {}, lastGtidSet is {}", expectedGtidSet, lastGtidSet);
            assertEquals(expectedGtidSet, lastGtidSet);
        }
    }

    private GtidSet getExpectedGtidSet(String initSourceId, String historyGtids, String expectedTransactionId) {
        String expectedGtids = "";
        if (historyGtids != null) {
            expectedGtids = expectedGtids + historyGtids + ",";
        }
        expectedGtids = expectedGtids + initSourceId + ":" + expectedTransactionId;
        return new GtidSet(expectedGtids);
    }

    static class GtidsDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        public void deserialize(SourceRecord record, Collector<String> out) {
            String sourceGtids = (String) record.sourceOffset().get("gtids");
            if (sourceGtids == null) {
                sourceGtids = "";
            }
            LOG.info("deserialize sourceGtids is {}", sourceGtids);
            out.collect(sourceGtids);
        }

        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.of(String.class);
        }

    }
}
