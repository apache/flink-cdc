/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Vendored from Debezium 3.4.2.Final. Emits change records based on an event read from Oracle
 * LogMiner.
 *
 * <p>Changes from the Flink CDC 3.6 / Debezium 1.9.x version:
 *
 * <ul>
 *   <li>Constructor reduced from 10 args to 9 args (rowId removed) to match the signature called by
 *       {@code UnbufferedLogMinerStreamingChangeEventSource.dispatchEvent()} in Debezium
 *       3.4.2.Final.
 *   <li>The {@code getEmitConnectHeaders()} override that injected the ROWID into Kafka Connect
 *       headers has been removed. {@code OracleSourceFetchTaskContext.isRecordBetween()} has been
 *       updated to handle the absent header by falling back to SCN-range inclusion. Tables without
 *       a primary key that use multi-chunk ROWID splits should set {@code
 *       scan.incremental.snapshot.chunk.key-column} explicitly.
 * </ul>
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final Operation operation;

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            Operation operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
    }

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            EventType eventType,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock) {
        this(
                connectorConfig,
                partition,
                offset,
                getOperation(eventType),
                oldValues,
                newValues,
                table,
                schema,
                clock);
    }

    private static Operation getOperation(EventType eventType) {
        switch (eventType) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
            case SELECT_LOB_LOCATOR:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + eventType);
        }
    }

    @Override
    public Operation getOperation() {
        return operation;
    }
}
