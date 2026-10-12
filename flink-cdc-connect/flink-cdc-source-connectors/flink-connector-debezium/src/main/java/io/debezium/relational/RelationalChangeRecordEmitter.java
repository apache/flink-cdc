/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * Vendored from Debezium. Constructor updated to use RelationalDatabaseConnectorConfig (Debezium
 * 3.4.2 API). Retains getEmitConnectHeaders() hook used by LogMinerChangeRecordEmitter to attach
 * ROWID headers.
 */
public abstract class RelationalChangeRecordEmitter<P extends Partition>
        extends AbstractChangeRecordEmitter<P, TableSchema> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RelationalChangeRecordEmitter.class);

    public static final String PK_UPDATE_OLDKEY_FIELD = "__debezium.oldkey";
    public static final String PK_UPDATE_NEWKEY_FIELD = "__debezium.newkey";

    public RelationalChangeRecordEmitter(
            P partition,
            OffsetContext offsetContext,
            Clock clock,
            RelationalDatabaseConnectorConfig connectorConfig) {
        super(partition, offsetContext, clock, connectorConfig);
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver<P> receiver)
            throws InterruptedException {
        TableSchema tableSchema = (TableSchema) schema;
        Operation operation = getOperation();

        switch (operation) {
            case CREATE:
                emitCreateRecord(receiver, tableSchema);
                break;
            case READ:
                emitReadRecord(receiver, tableSchema);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, tableSchema);
                break;
            case DELETE:
                emitDeleteRecord(receiver, tableSchema);
                break;
            case TRUNCATE:
                emitTruncateRecord(receiver, tableSchema);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    @Override
    protected void emitCreateRecord(Receiver<P> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .create(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.warn(
                    "no new values found for table '{}' from create message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.CREATE,
                newKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    @Override
    protected void emitReadRecord(Receiver<P> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .read(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());

        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.READ,
                newKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    @Override
    protected void emitUpdateRecord(Receiver<P> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.warn(
                    "no new values found for table '{}' from update message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            Struct envelope =
                    tableSchema
                            .getEnvelopeSchema()
                            .update(
                                    oldValue,
                                    newValue,
                                    getOffset().getSourceInfo(),
                                    getClock().currentTimeAsInstant());
            receiver.changeRecord(
                    getPartition(),
                    tableSchema,
                    Operation.UPDATE,
                    newKey,
                    envelope,
                    getOffset(),
                    getEmitConnectHeaders().orElse(null));
        } else {
            emitUpdateAsPrimaryKeyChangeRecord(
                    receiver, tableSchema, oldKey, newKey, oldValue, newValue);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver<P> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
            LOGGER.warn(
                    "no old values found for table '{}' from delete message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }

        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .delete(
                                oldValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.DELETE,
                oldKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    protected void emitTruncateRecord(Receiver<P> receiver, TableSchema schema)
            throws InterruptedException {
        throw new UnsupportedOperationException("TRUNCATE not supported");
    }

    protected abstract Object[] getOldColumnValues();

    protected abstract Object[] getNewColumnValues();

    protected boolean skipEmptyMessages() {
        return false;
    }

    protected void emitUpdateAsPrimaryKeyChangeRecord(
            Receiver<P> receiver,
            TableSchema tableSchema,
            Struct oldKey,
            Struct newKey,
            Struct oldValue,
            Struct newValue)
            throws InterruptedException {
        ConnectHeaders headers = getEmitConnectHeaders().orElse(new ConnectHeaders());
        headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .delete(
                                oldValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.DELETE,
                oldKey,
                envelope,
                getOffset(),
                headers);

        headers = getEmitConnectHeaders().orElse(new ConnectHeaders());
        headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

        envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .create(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.CREATE,
                newKey,
                envelope,
                getOffset(),
                headers);
    }

    protected Optional<ConnectHeaders> getEmitConnectHeaders() {
        return Optional.empty();
    }
}
