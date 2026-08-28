/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mysql.MySqlOffsetContext.Loader;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/*
 * Copied from Debezium project(2.7.4.Final).
 *
 * <p>Change 1: the binlog position is read with the statement probed by {@code MySqlConnection}
 * (MySQL 8.4 replaced {@code SHOW MASTER STATUS} with {@code SHOW BINARY LOG STATUS}).
 *
 * <p>The snapshot lifecycle patches live in the forked
 * {@code io.debezium.connector.binlog.BinlogSnapshotChangeEventSource}, which Debezium 2.7
 * extracted from this class.
 */
public class MySqlSnapshotChangeEventSource
        extends BinlogSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlSnapshotChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;

    public MySqlSnapshotChangeEventSource(
            MySqlConnectorConfig connectorConfig,
            MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
            MySqlDatabaseSchema schema,
            EventDispatcher<MySqlPartition, TableId> dispatcher,
            Clock clock,
            MySqlSnapshotChangeEventSourceMetrics metrics,
            BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
            Runnable preSnapshotAction,
            NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
            SnapshotterService snapshotterService) {
        super(
                connectorConfig,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                metrics,
                lastEventProcessor,
                preSnapshotAction,
                notificationService,
                snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected MySqlOffsetContext getInitialOffsetContext(BinlogConnectorConfig connectorConfig) {
        return MySqlOffsetContext.initial((MySqlConnectorConfig) connectorConfig);
    }

    @Override
    protected void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(
            MySqlOffsetContext offsetContext,
            BinlogConnectorConnection connection,
            SnapshotterService snapshotterService)
            throws Exception {
        LOGGER.info("Read binlog position of MySQL primary server");
        final String showMasterStmt = ((MySqlConnection) connection).getShowBinaryLogStatement();
        connection.query(
                showMasterStmt,
                rs -> {
                    if (rs.next()) {
                        final String binlogFilename = rs.getString(1);
                        final long binlogPosition = rs.getLong(2);
                        offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
                        if (rs.getMetaData().getColumnCount() > 4) {
                            // This column exists only in MySQL 5.6.5 or later ...
                            final String gtidSet =
                                    rs.getString(
                                            5); // GTID set, may be null, blank, or contain a GTID
                            // set
                            offsetContext.setCompletedGtidSet(gtidSet);
                            LOGGER.info(
                                    "\t using binlog '{}' at position '{}' and gtid '{}'",
                                    binlogFilename,
                                    binlogPosition,
                                    gtidSet);
                        }
                    } else if (!snapshotterService.getSnapshotter().shouldStream()) {
                        LOGGER.warn(
                                "Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
                    } else {
                        throw new DebeziumException(
                                "Cannot read the binlog filename and position via '"
                                        + showMasterStmt
                                        + "'. Make sure your server is correctly configured");
                    }
                });
    }

    @Override
    protected MySqlOffsetContext copyOffset(
            RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
