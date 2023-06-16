/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.source.read.fetch;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceTestBase;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import com.ververica.cdc.connectors.sqlserver.source.reader.fetch.SqlServerScanFetchTask;
import com.ververica.cdc.connectors.sqlserver.source.reader.fetch.SqlServerSourceFetchTaskContext;
import com.ververica.cdc.connectors.sqlserver.testutils.RecordsFormatter;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.createSqlServerConnection;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Tests for {@link SqlServerScanFetchTask}. */
public class SqlServerScanFetchTaskTest extends SqlServerSourceTestBase {

    @Test
    public void testChangingDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfigFactory);

        String tableId = databaseName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };

        MakeChangeEventTaskContext makeChangeEventTaskContext =
                new MakeChangeEventTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        () -> executeSql(sourceConfig, changingDataSql));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(snapshotSplits, makeChangeEventTaskContext, 1, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testInsertDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfigFactory);

        String tableId = databaseName + "." + tableName;
        String[] insertDataSql =
                new String[] {
                    "INSERT INTO " + tableId + " VALUES(112, 'user_12','Shanghai','123567891234')",
                    "INSERT INTO " + tableId + " VALUES(113, 'user_13','Shanghai','123567891234')",
                };

        MakeChangeEventTaskContext makeChangeEventTaskContext =
                new MakeChangeEventTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        () -> executeSql(sourceConfig, insertDataSql));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[112, user_12, Shanghai, 123567891234]",
                    "+I[113, user_13, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(snapshotSplits, makeChangeEventTaskContext, 1, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testDeleteDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfigFactory);

        String tableId = databaseName + "." + tableName;
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId + " where id = 101",
                    "DELETE FROM " + tableId + " where id = 102",
                };

        MakeChangeEventTaskContext makeChangeEventTaskContext =
                new MakeChangeEventTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        createSqlServerConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        () -> executeSql(sourceConfig, deleteDataSql));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(snapshotSplits, makeChangeEventTaskContext, 1, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            SqlServerSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            DataType dataType)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            SnapshotSplit sqlSplit = snapshotSplits.get(i);
            if (sourceScanFetcher.isFinished()) {
                sourceScanFetcher.submitTask(
                        taskContext.getDataSourceDialect().createFetchTask(sqlSplit));
            }
            Iterator<SourceRecords> res;
            while ((res = sourceScanFetcher.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecords sourceRecords = res.next();
                    result.addAll(sourceRecords.getSourceRecordList());
                }
            }
        }

        sourceScanFetcher.close();

        assertNotNull(sourceScanFetcher.getExecutorService());
        assertTrue(sourceScanFetcher.getExecutorService().isTerminated());

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            SqlServerSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect) {
        String databaseName = sourceConfig.getDatabaseList().get(0);
        List<TableId> tableIdList =
                sourceConfig.getTableList().stream()
                        .map(tableId -> TableId.parse(databaseName + "." + tableId))
                        .collect(Collectors.toList());
        final ChunkSplitter chunkSplitter = sourceDialect.createChunkSplitter(sourceConfig);

        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        for (TableId table : tableIdList) {
            Collection<SnapshotSplit> snapshotSplits = chunkSplitter.generateSplits(table);
            snapshotSplitList.addAll(snapshotSplits);
        }
        return snapshotSplitList;
    }

    public static SqlServerSourceConfigFactory getConfigFactory(
            String databaseName, String[] captureTables, int splitSize) {
        return (SqlServerSourceConfigFactory)
                new SqlServerSourceConfigFactory()
                        .hostname(MSSQL_SERVER_CONTAINER.getHost())
                        .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                        .username(MSSQL_SERVER_CONTAINER.getUsername())
                        .password(MSSQL_SERVER_CONTAINER.getPassword())
                        .databaseList(databaseName)
                        .tableList(captureTables)
                        .splitSize(splitSize);
    }

    private boolean executeSql(SqlServerSourceConfig sourceConfig, String[] sqlStatements) {
        JdbcConnection connection =
                createSqlServerConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig());
        try {
            connection.setAutoCommit(false);
            connection.execute(sqlStatements);
            connection.commit();
        } catch (SQLException e) {
            LOG.error("Failed to execute sql statements.", e);
            return false;
        }
        return true;
    }

    class MakeChangeEventTaskContext extends SqlServerSourceFetchTaskContext {

        private Supplier<Boolean> makeChangeEventFunction;

        public MakeChangeEventTaskContext(
                JdbcSourceConfig jdbcSourceConfig,
                SqlServerDialect sqlServerDialect,
                SqlServerConnection connection,
                SqlServerConnection metaDataConnection,
                Supplier<Boolean> makeChangeEventFunction) {
            super(jdbcSourceConfig, sqlServerDialect, connection, metaDataConnection);
            this.makeChangeEventFunction = makeChangeEventFunction;
        }

        @Override
        public EventDispatcher.SnapshotReceiver getSnapshotReceiver() {
            EventDispatcher.SnapshotReceiver snapshotReceiver = super.getSnapshotReceiver();
            return new EventDispatcher.SnapshotReceiver() {

                @Override
                public void changeRecord(
                        DataCollectionSchema schema,
                        Envelope.Operation operation,
                        Object key,
                        Struct value,
                        OffsetContext offset,
                        ConnectHeaders headers)
                        throws InterruptedException {
                    snapshotReceiver.changeRecord(schema, operation, key, value, offset, headers);
                }

                @Override
                public void completeSnapshot() throws InterruptedException {
                    snapshotReceiver.completeSnapshot();
                    // make change events
                    makeChangeEventFunction.get();
                    Thread.sleep(10 * 1000);
                }
            };
        }
    }
}
