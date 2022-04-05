package io.debezium.connector.mysql;
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine.CompletionResult;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

/** Copied from Debezium io.debezium.connector.mysql.MySqlConnectorIT to cover DBZ-3949. */
@SkipWhenDatabaseVersion(
        check = LESS_THAN,
        major = 5,
        minor = 6,
        reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class MySqlConnectorIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH =
            Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private static final UniqueDatabase DATABASE =
            new UniqueDatabase("myServer1", "connector_test").withDbHistoryPath(DB_HISTORY_PATH);
    private static final UniqueDatabase RO_DATABASE =
            new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
                    .withDbHistoryPath(DB_HISTORY_PATH);
    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    private void waitForStreamingRunning(String serverName) throws InterruptedException {
        waitForStreamingRunning("mysql", serverName, getStreamingNamespace());
    }

    @Test
    @FixFor("DBZ-3949")
    public void testDmlInChangeEvents() throws Exception {
        config =
                DATABASE.defaultConfig()
                        .with(
                                MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                                DATABASE.qualifiedTableName("products"))
                        .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                        .with(
                                MySqlConnectorConfig.SNAPSHOT_MODE,
                                MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                        .with(
                                MySqlConnectorConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE,
                                CommonConnectorConfig.EventProcessingFailureHandlingMode.FAIL)
                        .build();

        // Start the connector.
        CompletionResult completion = new CompletionResult();
        start(MySqlConnector.class, config, completion);
        waitForStreamingRunning(DATABASE.getServerName());

        // Do some changes.
        try (MySqlTestConnection db =
                MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()); ) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "INSERT INTO products VALUES (204,'rubberduck','Rubber Duck',2.12);");
                connection.execute(
                        "INSERT INTO products VALUES (205,'rubbercrocodile','Rubber Crocodile',4.14);");
                connection.execute(
                        "INSERT INTO products VALUES (206,'rubberfish','Rubber Fish',5.15);");
            }
        }

        // Switch to 'STATEMENT' binlog format to mimic DML events in the log.
        try (MySqlTestConnection db =
                MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        String.format(
                                "SET GLOBAL binlog_format = 'STATEMENT'",
                                DATABASE.getDatabaseName()));
            }
        }

        // Do some more changes.
        try (MySqlTestConnection db =
                MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()); ) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET weight=2.22 WHERE id=204;");
                connection.execute("UPDATE products SET weight=4.44 WHERE id=205;");
                connection.execute("UPDATE products SET weight=5.55 WHERE id=206;");
            }
        }

        // Last 3 changes should be ignored as they were stored using STATEMENT format.
        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> changeEvents =
                records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(changeEvents.size()).isEqualTo(3);
        // There shouldn't be any error.
        assertThat(completion.hasError()).isFalse();

        // Switch back to 'ROW' binlog format.
        try (MySqlTestConnection db =
                MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        String.format(
                                "SET GLOBAL binlog_format = 'ROW'", DATABASE.getDatabaseName()));
            }
        }

        stopConnector();
    }
}
