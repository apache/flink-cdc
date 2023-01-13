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

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;

import com.fasterxml.jackson.core.JsonParseException;
import com.jayway.jsonpath.JsonPath;
import com.ververica.cdc.connectors.mysql.MySqlTestUtils.TestingListState;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.connectors.utils.TestSourceContext;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.basicSourceBuilder;
import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.drain;
import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.setupSource;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertDelete;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertInsert;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertRead;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertUpdate;
import static com.ververica.cdc.debezium.utils.DatabaseHistoryUtil.removeHistory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the legacy {@link MySqlSource} which also heavily tests {@link DebeziumSourceFunction}.
 */
@RunWith(Parameterized.class)
public class LegacyMySqlSourceTest extends LegacyMySqlTestBase {

    private final UniqueDatabase database =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Override
    public String getTempFilePath(String fileName) throws IOException {
        return super.getTempFilePath(fileName);
    }

    @Parameterized.Parameter public boolean useLegacyImplementation;

    @Parameterized.Parameters(name = "UseLegacyImplementation: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Before
    public void before() {
        database.createAndInitialize();
    }

    @Test
    public void testConsumingAllEvents() throws Exception {
        DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();

        setupSource(source);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // start the source
            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            List<SourceRecord> records = drain(sourceContext, 9);
            assertEquals(9, records.size());
            for (int i = 0; i < records.size(); i++) {
                if (this.useLegacyImplementation) {
                    assertInsert(records.get(i), "id", 101 + i);
                } else {
                    assertRead(records.get(i), "id", 101 + i);
                }
            }

            statement.execute(
                    "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)"); // 110
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 110);

            statement.execute(
                    "INSERT INTO products VALUES (1001,'roy','old robot',1234.56)"); // 1001
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 1001);

            // ---------------------------------------------------------------------------------------------------------------
            // Changing the primary key of a row should result in 2 events: INSERT, DELETE
            // (TOMBSTONE is dropped)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute(
                    "UPDATE products SET id=2001, description='really old robot' WHERE id=1001");
            records = drain(sourceContext, 2);
            assertDelete(records.get(0), "id", 1001);
            assertInsert(records.get(1), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Simple UPDATE (with no schema changes)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute("UPDATE products SET weight=1345.67 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Change our schema with a fully-qualified name; we should still see this event
            // ---------------------------------------------------------------------------------------------------------------
            // Add a column with default to the 'products' table and explicitly update one record
            // ...
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.products ADD COLUMN volume FLOAT, ADD COLUMN alias VARCHAR(30) NULL AFTER description",
                            database.getDatabaseName()));
            statement.execute("UPDATE products SET volume=13.5 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // cleanup
            source.cancel();
            source.close();
            runThread.sync();
        }
    }

    @Test
    public void testCheckpointAndRestore() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        int prevPos = 0;
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
            // we use blocking context to block the source to emit before last snapshot record
            final BlockingSourceContext<SourceRecord> sourceContext =
                    new BlockingSourceContext<>(8);
            // setup source with empty state
            setupSource(source, false, offsetState, historyState, true, 0, 1);

            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            // wait until consumer is started
            int received = drain(sourceContext, 2).size();
            assertEquals(2, received);

            // we can't perform checkpoint during DB snapshot
            assertFalse(
                    waitForCheckpointLock(
                            sourceContext.getCheckpointLock(), Duration.ofSeconds(3)));

            // unblock the source context to continue the processing
            sourceContext.blocker.release();
            // wait until the source finishes the database snapshot
            List<SourceRecord> records = drain(sourceContext, 9 - received);
            assertEquals(9, records.size() + received);

            // state is still empty
            assertEquals(0, offsetState.list.size());
            assertEquals(0, historyState.list.size());

            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1 after snapshot finished
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                // trigger checkpoint-1
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }

            assertHistoryState(historyState);
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
            assertFalse(state.contains("row"));
            assertFalse(state.contains("server_id"));
            assertFalse(state.contains("event"));
            int pos = JsonPath.read(state, "$.sourceOffset.pos");
            assertTrue(pos > prevPos);
            prevPos = pos;

            source.cancel();
            source.close();
            runThread.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-3: restore the source from state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source2 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            setupSource(source2, true, offsetState, historyState, true, 0, 1);
            final CheckedThread runThread2 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source2.run(sourceContext2);
                        }
                    };
            runThread2.start();

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext2));

            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)"); // 110
                List<SourceRecord> records = drain(sourceContext2, 1);
                assertEquals(1, records.size());
                assertInsert(records.get(0), "id", 110);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext2.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source2.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertHistoryState(historyState); // assert the DDL is stored in the history state
                assertEquals(1, offsetState.list.size());
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                assertEquals(
                        "mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
                assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
                assertEquals("1", JsonPath.read(state, "$.sourceOffset.row").toString());
                assertEquals("223344", JsonPath.read(state, "$.sourceOffset.server_id").toString());
                assertEquals("2", JsonPath.read(state, "$.sourceOffset.event").toString());
                int pos = JsonPath.read(state, "$.sourceOffset.pos");
                assertTrue(pos > prevPos);
                prevPos = pos;

                // execute 2 more DMLs to have more binlog
                statement.execute(
                        "INSERT INTO products VALUES (1001,'roy','old robot',1234.56)"); // 1001
                statement.execute("UPDATE products SET weight=1345.67 WHERE id=1001");
            }

            // cancel the source
            source2.cancel();
            source2.close();
            runThread2.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-5: restore the source from checkpoint-2
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source3 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext3 = new TestSourceContext<>();
            setupSource(source3, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread3 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source3.run(sourceContext3);
                        }
                    };
            runThread3.start();

            // consume the unconsumed binlog
            List<SourceRecord> records = drain(sourceContext3, 2);
            assertInsert(records.get(0), "id", 1001);
            assertUpdate(records.get(1), "id", 1001);

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext3));

            // can continue to receive new events
            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM products WHERE id=1001");
            }
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "id", 1001);

            // ---------------------------------------------------------------------------
            // Step-6: trigger checkpoint-2 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext3.getCheckpointLock()) {
                // checkpoint 3
                source3.snapshotState(new StateSnapshotContextSynchronousImpl(233, 233));
            }
            assertHistoryState(historyState); // assert the DDL is stored in the history state
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
            assertEquals("1", JsonPath.read(state, "$.sourceOffset.row").toString());
            assertEquals("223344", JsonPath.read(state, "$.sourceOffset.server_id").toString());
            assertEquals("2", JsonPath.read(state, "$.sourceOffset.event").toString());
            int pos = JsonPath.read(state, "$.sourceOffset.pos");
            assertTrue(pos > prevPos);

            source3.cancel();
            source3.close();
            runThread3.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-7: restore the source from checkpoint-3
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source4 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext4 = new TestSourceContext<>();
            setupSource(source4, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread4 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source4.run(sourceContext4);
                        }
                    };
            runThread4.start();

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext4));

            // ---------------------------------------------------------------------------
            // Step-8: trigger checkpoint-3 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext4.getCheckpointLock()) {
                // checkpoint 4
                source4.snapshotState(new StateSnapshotContextSynchronousImpl(254, 254));
            }
            assertHistoryState(historyState); // assert the DDL is stored in the history state
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
            assertEquals("1", JsonPath.read(state, "$.sourceOffset.row").toString());
            assertEquals("223344", JsonPath.read(state, "$.sourceOffset.server_id").toString());
            assertEquals("2", JsonPath.read(state, "$.sourceOffset.event").toString());
            int pos = JsonPath.read(state, "$.sourceOffset.pos");
            assertTrue(pos > prevPos);

            source4.cancel();
            source4.close();
            runThread4.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-9: insert partial and alter table
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source5 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext5 = new TestSourceContext<>();
            setupSource(source5, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread5 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source5.run(sourceContext5);
                        }
                    };
            runThread5.start();

            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO products(id, description, weight) VALUES (default, 'Go go go', 111.1)");
                statement.execute(
                        "ALTER TABLE products ADD comment_col VARCHAR(100) DEFAULT 'cdc'");
                List<SourceRecord> records = drain(sourceContext5, 1);
                assertInsert(records.get(0), "id", 1002);
            }

            // ---------------------------------------------------------------------------
            // Step-10: trigger checkpoint-4
            // ---------------------------------------------------------------------------
            synchronized (sourceContext5.getCheckpointLock()) {
                // trigger checkpoint-4
                source5.snapshotState(new StateSnapshotContextSynchronousImpl(300, 300));
            }
            assertHistoryState(historyState); // assert the DDL is stored in the history state
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
            assertEquals("1", JsonPath.read(state, "$.sourceOffset.row").toString());
            assertEquals("223344", JsonPath.read(state, "$.sourceOffset.server_id").toString());
            assertEquals("2", JsonPath.read(state, "$.sourceOffset.event").toString());
            int pos = JsonPath.read(state, "$.sourceOffset.pos");
            assertTrue(pos > prevPos);

            source5.cancel();
            source5.close();
            runThread5.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-11: restore from the checkpoint-4 and insert the partial value
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source6 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext6 = new TestSourceContext<>();
            setupSource(source6, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread6 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source6.run(sourceContext6);
                        }
                    };
            runThread6.start();
            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO products(id, description, weight) VALUES (default, 'Run!', 22.2)");
                List<SourceRecord> records = drain(sourceContext6, 1);
                assertInsert(records.get(0), "id", 1003);
            }

            source6.cancel();
            source6.close();
            runThread6.sync();
        }
    }

    @Test
    public void testRecoverFromRenameOperation() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        {
            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // Step-1: start the source from empty state
                final DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
                final TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();
                // setup source with empty state
                setupSource(source, false, offsetState, historyState, true, 0, 1);

                final CheckedThread runThread =
                        new CheckedThread() {
                            @Override
                            public void go() throws Exception {
                                source.run(sourceContext);
                            }
                        };
                runThread.start();

                // wait until the source finishes the database snapshot
                List<SourceRecord> records = drain(sourceContext, 9);
                assertEquals(9, records.size());

                // state is still empty
                assertEquals(0, offsetState.list.size());
                assertEquals(0, historyState.list.size());

                // create temporary tables which are not in the whitelist
                statement.execute("CREATE TABLE `tp_001_ogt_products` LIKE `products`;");
                // do some renames
                statement.execute(
                        "RENAME TABLE `products` TO `tp_001_del_products`, `tp_001_ogt_products` TO `products`;");

                statement.execute(
                        "INSERT INTO `products` VALUES (110,'robot','Toy robot',1.304)"); // 110
                statement.execute(
                        "INSERT INTO `products` VALUES (111,'stream train','Town stream train',1.304)"); // 111
                statement.execute(
                        "INSERT INTO `products` VALUES (112,'cargo train','City cargo train',1.304)"); // 112

                int received = drain(sourceContext, 3).size();
                assertEquals(3, received);

                // Step-2: trigger a checkpoint
                synchronized (sourceContext.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
                }

                assertTrue(historyState.list.size() > 0);
                assertTrue(offsetState.list.size() > 0);

                source.cancel();
                source.close();
                runThread.sync();
            }
        }

        {
            // Step-3: restore the source from state
            final DebeziumSourceFunction<SourceRecord> source2 = createMySqlBinlogSource();
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            setupSource(source2, true, offsetState, historyState, true, 0, 1);
            final CheckedThread runThread2 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source2.run(sourceContext2);
                        }
                    };
            runThread2.start();

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext2));

            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO `products` VALUES (113,'Airplane','Toy airplane',1.304)"); // 113
                List<SourceRecord> records = drain(sourceContext2, 1);
                assertEquals(1, records.size());
                assertInsert(records.get(0), "id", 113);

                source2.cancel();
                source2.close();
                runThread2.sync();
            }
        }
    }

    @Test
    public void testStartupFromSpecificOffset() throws Exception {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)"); // 110
        }

        Tuple2<String, Integer> offset =
                currentMySqlLatestOffset(
                        MYSQL_CONTAINER, database, "products", 10, useLegacyImplementation);
        final String offsetFile = offset.f0;
        final int offsetPos = offset.f1;
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        // ---------------------------------------------------------------------------
        // Step-3: start source from the specific offset
        // ---------------------------------------------------------------------------
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO products VALUES (1001,'roy','old robot',1234.56)"); // 1001
            statement.execute(
                    "UPDATE products SET id=2001, description='really old robot' WHERE id=1001");
            statement.execute("UPDATE products SET weight=1345.67 WHERE id=2001");

            final DebeziumSourceFunction<SourceRecord> source2 =
                    createMySqlBinlogSource(offsetFile, offsetPos);
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            setupSource(source2, false, offsetState, historyState, true, 0, 1);
            final CheckedThread runThread2 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source2.run(sourceContext2);
                        }
                    };
            runThread2.start();

            // we can continue to read binlog from specific offset
            List<SourceRecord> records = drain(sourceContext2, 4);
            assertEquals(4, records.size());
            assertInsert(records.get(0), "id", 1001);
            assertDelete(records.get(1), "id", 1001);
            assertInsert(records.get(2), "id", 2001);
            assertUpdate(records.get(3), "id", 2001);

            // ---------------------------------------------------------------------------
            // Step-4: trigger checkpoint-2
            // ---------------------------------------------------------------------------
            synchronized (sourceContext2.getCheckpointLock()) {
                // trigger checkpoint-2
                source2.snapshotState(new StateSnapshotContextSynchronousImpl(201, 201));
            }

            source2.cancel();
            source2.close();
            runThread2.sync();
        }

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // --------------------------------------------------------------------------------
            // Step-5: restore from last checkpoint to verify not restore from specific offset
            // --------------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source3 =
                    createMySqlBinlogSource(offsetFile, offsetPos);
            final TestSourceContext<SourceRecord> sourceContext3 = new TestSourceContext<>();
            setupSource(source3, true, offsetState, historyState, true, 0, 1);
            final CheckedThread runThread3 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source3.run(sourceContext3);
                        }
                    };
            runThread3.start();

            statement.execute("DELETE FROM products WHERE id=2001");
            List<SourceRecord> records = drain(sourceContext3, 1);
            assertEquals(1, records.size());
            assertDelete(records.get(0), "id", 2001);

            source3.cancel();
            source3.close();
            runThread3.sync();
        }
    }

    @Test
    public void testConsumingEmptyTable() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        int prevPos = 0;
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder(database, "UTC", useLegacyImplementation)
                            .tableList(database.getDatabaseName() + "." + "category")
                            .build();
            // we use blocking context to block the source to emit before last snapshot record
            final BlockingSourceContext<SourceRecord> sourceContext =
                    new BlockingSourceContext<>(8);
            // setup source with empty state
            setupSource(source, false, offsetState, historyState, true, 0, 1);

            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            // wait until Debezium is started
            while (!source.getDebeziumStarted()) {
                Thread.sleep(100);
            }
            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }

            // state is still empty
            assertEquals(0, offsetState.list.size());

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext));

            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute("INSERT INTO category VALUES (1, 'book')");
                statement.execute("INSERT INTO category VALUES (2, 'shoes')");
                statement.execute("UPDATE category SET category_name='books' WHERE id=1");
                List<SourceRecord> records = drain(sourceContext, 3);
                assertEquals(3, records.size());
                assertInsert(records.get(0), "id", 1);
                assertInsert(records.get(1), "id", 2);
                assertUpdate(records.get(2), "id", 1);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertHistoryState(historyState); // assert the DDL is stored in the history state
                assertEquals(1, offsetState.list.size());
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                assertEquals(
                        "mysql_binlog_source", JsonPath.read(state, "$.sourcePartition.server"));
                assertEquals("mysql-bin.000003", JsonPath.read(state, "$.sourceOffset.file"));
                assertEquals("1", JsonPath.read(state, "$.sourceOffset.row").toString());
                assertEquals("223344", JsonPath.read(state, "$.sourceOffset.server_id").toString());
                assertEquals("2", JsonPath.read(state, "$.sourceOffset.event").toString());
                int pos = JsonPath.read(state, "$.sourceOffset.pos");
                assertTrue(pos > prevPos);
            }

            source.cancel();
            source.close();
            runThread.sync();
        }
    }

    @Test
    public void testChooseDatabase() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        historyState.add("engine-name");
        DocumentWriter writer = DocumentWriter.defaultWriter();
        if (useLegacyImplementation) {
            // build a non-legacy state
            FlinkJsonTableChangeSerializer tableChangesSerializer =
                    new FlinkJsonTableChangeSerializer();
            historyState.add(
                    writer.write(
                            tableChangesSerializer.toDocument(
                                    new TableChanges.TableChange(
                                            TableChanges.TableChangeType.CREATE,
                                            MockedTable.INSTANCE))));
        } else {
            // build a legacy state
            Document document =
                    new HistoryRecord(
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    "test",
                                    "test",
                                    "CREATE TABLE test(a int)",
                                    null)
                            .document();
            historyState.add(writer.write(document));
        }

        final DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
        setupSource(source, true, offsetState, historyState, true, 0, 1);

        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();

        final CheckedThread runThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source.run(sourceContext);
                    }
                };
        runThread.start();
        if (useLegacyImplementation) {
            // should fail because user specifies to use the legacy implementation
            try {
                runThread.sync();
                fail("Should fail.");
            } catch (Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals(
                        "The configured option 'debezium.internal.implementation' is 'legacy', but the state of source is incompatible with this implementation, you should remove the the option.",
                        e.getMessage());
            }
        } else {
            // check the debezium status to verify
            waitDebeziumStartWithTimeout(source, 5_000L);

            source.cancel();
            source.close();
            runThread.sync();
        }
    }

    @Test
    public void testLoadIllegalState() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        historyState.add("engine-name");
        historyState.add("IllegalState");

        final DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
        try {
            setupSource(source, true, offsetState, historyState, true, 0, 1);
            fail("Should fail.");
        } catch (Exception e) {
            assertTrue(e instanceof JsonParseException);
            assertTrue(e.getMessage().contains("Unrecognized token 'IllegalState'"));
        }
    }

    @Test
    public void testSchemaRemovedBeforeCheckpoint() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        {
            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // Step-1: start the source from empty state
                final DebeziumSourceFunction<SourceRecord> source = createMySqlBinlogSource();
                final TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();
                // setup source with empty state
                setupSource(source, false, offsetState, historyState, true, 0, 1);

                final CheckedThread runThread =
                        new CheckedThread() {
                            @Override
                            public void go() throws Exception {
                                source.run(sourceContext);
                            }
                        };
                runThread.start();

                // wait until the source finishes the database snapshot
                List<SourceRecord> records = drain(sourceContext, 9);
                assertEquals(9, records.size());

                // state is still empty
                assertEquals(0, offsetState.list.size());
                assertEquals(0, historyState.list.size());

                statement.execute(
                        "INSERT INTO `products` VALUES (110,'robot','Toy robot',1.304)"); // 110

                int received = drain(sourceContext, 1).size();
                assertEquals(1, received);

                // Step-2: trigger a checkpoint
                synchronized (sourceContext.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
                }

                assertTrue(historyState.list.size() > 0);
                assertTrue(offsetState.list.size() > 0);

                // Step-3: mock the engine stop, remove the schema history before checkpoint
                final String engineInstanceName = source.getEngineInstanceName();
                removeHistory(engineInstanceName);

                try {
                    synchronized (sourceContext.getCheckpointLock()) {
                        // trigger checkpoint-2
                        source.snapshotState(new StateSnapshotContextSynchronousImpl(102, 102));
                    }
                    fail("Should fail.");
                } catch (Exception e) {
                    assertTrue(e instanceof IllegalStateException);
                    assertTrue(
                            e.getMessage()
                                    .contains(
                                            String.format(
                                                    "Retrieve schema history failed, the schema records for engine %s has been removed,"
                                                            + " this might because the debezium engine has been shutdown due to other errors.",
                                                    engineInstanceName)));
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Public Utilities
    // ------------------------------------------------------------------------------------------

    /** Gets the latest offset of current MySQL server. */
    public static Tuple2<String, Integer> currentMySqlLatestOffset(
            MySqlContainer container,
            UniqueDatabase database,
            String table,
            int expectedRecordCount,
            boolean useLegacyImplementation)
            throws Exception {
        DebeziumSourceFunction<SourceRecord> source =
                MySqlSource.<SourceRecord>builder()
                        .hostname(container.getHost())
                        .port(container.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .tableList(database.getDatabaseName() + "." + table)
                        .username(container.getUsername())
                        .password(container.getPassword())
                        .deserializer(new MySqlTestUtils.ForwardDeserializeSchema())
                        .debeziumProperties(createDebeziumProperties(useLegacyImplementation))
                        .build();
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        // ---------------------------------------------------------------------------
        // Step-1: start source
        // ---------------------------------------------------------------------------
        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();
        setupSource(source, false, offsetState, historyState, true, 0, 1);
        final CheckedThread runThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source.run(sourceContext);
                    }
                };
        runThread.start();

        drain(sourceContext, expectedRecordCount);

        // ---------------------------------------------------------------------------
        // Step-2: trigger checkpoint-1 after snapshot finished
        // ---------------------------------------------------------------------------
        synchronized (sourceContext.getCheckpointLock()) {
            // trigger checkpoint-1
            source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
        }

        assertEquals(1, offsetState.list.size());
        String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
        String offsetFile = JsonPath.read(state, "$.sourceOffset.file");
        int offsetPos = JsonPath.read(state, "$.sourceOffset.pos");

        source.cancel();
        source.close();
        runThread.sync();

        return Tuple2.of(offsetFile, offsetPos);
    }

    private static Properties createDebeziumProperties(boolean useLegacyImplementation) {
        Properties debeziumProps = new Properties();
        if (useLegacyImplementation) {
            debeziumProps.put("internal.implementation", "legacy");
            // check legacy mysql record type
            debeziumProps.put("transforms", "snapshotasinsert");
            debeziumProps.put(
                    "transforms.snapshotasinsert.type",
                    "io.debezium.connector.mysql.transforms.ReadToInsertEvent");
        }
        return debeziumProps;
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void waitDebeziumStartWithTimeout(
            DebeziumSourceFunction<SourceRecord> source, Long timeout) throws Exception {
        long start = System.currentTimeMillis();
        long end = start + timeout;
        while (!source.getDebeziumStarted()) {
            Thread.sleep(100);
            long now = System.currentTimeMillis();
            if (now > end) {
                fail("Should fail.");
            }
        }
    }

    private void assertHistoryState(TestingListState<String> historyState) {
        assertTrue(historyState.list.size() > 0);
        // assert the DDL is stored in the history state
        if (!useLegacyImplementation) {
            boolean hasTable =
                    historyState.list.stream()
                            .skip(1)
                            .anyMatch(
                                    history ->
                                            !((Map<?, ?>) JsonPath.read(history, "$.table"))
                                                            .isEmpty()
                                                    && (JsonPath.read(history, "$.type")
                                                                    .toString()
                                                                    .equals("CREATE")
                                                            || JsonPath.read(history, "$.type")
                                                                    .toString()
                                                                    .equals("ALTER")));
            assertTrue(hasTable);
        } else {
            boolean hasDDL =
                    historyState.list.stream()
                            .skip(1)
                            .anyMatch(
                                    history ->
                                            JsonPath.read(history, "$.source.server")
                                                            .equals("mysql_binlog_source")
                                                    && JsonPath.read(history, "$.position.snapshot")
                                                            .toString()
                                                            .equals("true")
                                                    && JsonPath.read(history, "$.ddl")
                                                            .toString()
                                                            .startsWith("CREATE TABLE `products`"));
            assertTrue(hasDDL);
        }
    }

    private DebeziumSourceFunction<SourceRecord> createMySqlBinlogSource(
            String offsetFile, int offsetPos) {
        return basicSourceBuilder(database, "UTC", useLegacyImplementation)
                .startupOptions(StartupOptions.specificOffset(offsetFile, offsetPos))
                .build();
    }

    private DebeziumSourceFunction<SourceRecord> createMySqlBinlogSource() {
        return basicSourceBuilder(database, "UTC", useLegacyImplementation).build();
    }

    private boolean waitForCheckpointLock(Object checkpointLock, Duration timeout)
            throws Exception {
        final Semaphore semaphore = new Semaphore(0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(
                () -> {
                    synchronized (checkpointLock) {
                        semaphore.release();
                    }
                });
        boolean result = semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
        executor.shutdownNow();
        return result;
    }

    /**
     * Wait for a maximum amount of time until the first record is available.
     *
     * @param timeout the maximum amount of time to wait; must not be negative
     * @return {@code true} if records are available, or {@code false} if the timeout occurred and
     *     no records are available
     */
    private boolean waitForAvailableRecords(Duration timeout, TestSourceContext<?> sourceContext)
            throws InterruptedException {
        long now = System.currentTimeMillis();
        long stop = now + timeout.toMillis();
        while (System.currentTimeMillis() < stop) {
            if (!sourceContext.getCollectedOutputs().isEmpty()) {
                break;
            }
            Thread.sleep(10); // save CPU
        }
        return !sourceContext.getCollectedOutputs().isEmpty();
    }

    private static class BlockingSourceContext<T> extends TestSourceContext<T> {

        private final Semaphore blocker = new Semaphore(0);
        private final int expectedCount;
        private int currentCount = 0;

        private BlockingSourceContext(int expectedCount) {
            this.expectedCount = expectedCount;
        }

        @Override
        public void collect(T t) {
            super.collect(t);
            currentCount++;
            if (currentCount == expectedCount) {
                try {
                    // block the source to emit records
                    blocker.acquire();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    private static class MockedTable implements Table {

        private static final Table INSTANCE = new MockedTable();

        private MockedTable() {}

        @Override
        public TableId id() {
            return TableId.parse("Test");
        }

        @Override
        public List<String> primaryKeyColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<String> retrieveColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<Column> columns() {
            return Collections.emptyList();
        }

        @Override
        public Column columnWithName(String name) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String defaultCharsetName() {
            return "UTF-8";
        }

        @Override
        public TableEditor edit() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
