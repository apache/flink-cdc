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
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.util.Optional;

/**
 * Copied from Debezium 1.9.8.Final. Emits change records based on an event read from Oracle
 * LogMiner.
 *
 * <p>This class add RowId and overrides the emit methods to put rowId in the header.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final Operation operation;
    private final String rowId;

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            Operation operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock,
            String rowId) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
        this.rowId = rowId;
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
            Clock clock,
            String rowId) {
        this(
                connectorConfig,
                partition,
                offset,
                getOperation(eventType),
                oldValues,
                newValues,
                table,
                schema,
                clock,
                rowId);
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

    @Override
    protected Optional<ConnectHeaders> getEmitConnectHeaders() {
        ConnectHeaders headers = new ConnectHeaders();
        headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));
        return Optional.of(headers);
    }
}
