/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractColumnValue;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.util.Strings;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message decoder for GaussDB's {@code mppdb_decoding} logical decoding output.
 *
 * <p>Expected JSON format:
 *
 * <pre>
 * {
 *   "table_name": "schema.table",
 *   "op_type": "INSERT|UPDATE|DELETE",
 *   "columns_name": ["col1", "col2"],
 *   "columns_type": ["integer", "varchar"],
 *   "columns_val": ["1", "'value'"],
 *   "old_keys_name": [],
 *   "old_keys_type": [],
 *   "old_keys_val": []
 * }
 * </pre>
 */
public class MppdbDecodingMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(MppdbDecodingMessageDecoder.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final long UNDEFINED_TX_ID = -1L;
    private static final Pattern TX_MARKER_PATTERN =
            Pattern.compile("^(?i)(BEGIN|COMMIT)(?:\\s+([0-9]+))?.*$");

    private Long currentTransactionId;
    private Instant currentTransactionTimestamp;

    @Override
    protected void processNotEmptyMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        final byte[] content = copyToByteArray(buffer);
        final String message = new String(content, StandardCharsets.UTF_8).trim();
        if (message.isEmpty()) {
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        if (!message.startsWith("{")) {
            if (tryProcessTransactionMarker(message, processor)) {
                return;
            }
            LOG.debug("Skipping non-JSON mppdb_decoding message: {}", message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        final JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(content);
        } catch (Exception e) {
            LOG.warn("Failed to parse mppdb_decoding JSON message, skipping: {}", message, e);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        final String opType = textValue(root, "op_type");
        if (opType == null || opType.isEmpty()) {
            LOG.warn("Missing op_type in mppdb_decoding message, skipping: {}", message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        if ("BEGIN".equalsIgnoreCase(opType) || "COMMIT".equalsIgnoreCase(opType)) {
            processTransactionJsonMessage(root, opType, processor);
            return;
        }

        final ReplicationMessage.Operation operation = parseOperation(opType);
        if (operation == null) {
            LOG.warn(
                    "Unsupported op_type '{}' in mppdb_decoding message, skipping: {}",
                    opType,
                    message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        final String tableName = textValue(root, "table_name");
        if (tableName == null || tableName.isEmpty()) {
            LOG.warn("Missing table_name in mppdb_decoding message, skipping: {}", message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        final JsonNode columnsName = root.get("columns_name");
        final JsonNode columnsType = root.get("columns_type");
        final JsonNode columnsVal = root.get("columns_val");
        final JsonNode oldKeysName = root.get("old_keys_name");
        final JsonNode oldKeysType = root.get("old_keys_type");
        final JsonNode oldKeysVal = root.get("old_keys_val");

        final List<ReplicationMessage.Column> columns =
                toColumns(typeRegistry, columnsName, columnsType, columnsVal, message);
        final List<ReplicationMessage.Column> newTuple =
                operation == ReplicationMessage.Operation.DELETE ? null : columns;
        final List<ReplicationMessage.Column> oldKeys =
                toColumns(typeRegistry, oldKeysName, oldKeysType, oldKeysVal, message);

        if (operation != ReplicationMessage.Operation.DELETE && columns == null) {
            LOG.warn(
                    "Missing or invalid columns_* fields in mppdb_decoding message, skipping: {}",
                    message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }
        if (operation == ReplicationMessage.Operation.DELETE
                && (columns == null || columns.isEmpty())
                && (oldKeys == null || oldKeys.isEmpty())) {
            LOG.warn(
                    "Missing both columns_* and old_keys_* fields for DELETE in mppdb_decoding message, skipping: {}",
                    message);
            processor.process(
                    new ReplicationMessage.NoopMessage(currentTransactionId, Instant.now()));
            return;
        }

        final List<ReplicationMessage.Column> oldTuple;
        if (operation == ReplicationMessage.Operation.INSERT) {
            oldTuple = null;
        } else if (operation == ReplicationMessage.Operation.DELETE) {
            oldTuple = oldKeys != null && !oldKeys.isEmpty() ? oldKeys : columns;
        } else {
            // UPDATE
            oldTuple = oldKeys != null && !oldKeys.isEmpty() ? oldKeys : null;
        }

        processor.process(
                new MppdbDecodingReplicationMessage(
                        operation,
                        currentTransactionId,
                        currentTransactionTimestamp != null
                                ? currentTransactionTimestamp
                                : Instant.now(),
                        normalizeTableName(tableName),
                        oldTuple,
                        newTuple,
                        true));
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(
            ChainedLogicalStreamBuilder builder,
            Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(
            ChainedLogicalStreamBuilder builder,
            Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    @Override
    public void close() {
        currentTransactionId = null;
        currentTransactionTimestamp = null;
    }

    private static byte[] copyToByteArray(ByteBuffer buffer) {
        ByteBuffer readBuffer = buffer.duplicate();
        byte[] bytes = new byte[readBuffer.remaining()];
        readBuffer.get(bytes);
        return bytes;
    }

    private boolean tryProcessTransactionMarker(
            String message, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        final Matcher matcher = TX_MARKER_PATTERN.matcher(message.trim());
        if (!matcher.matches()) {
            return false;
        }

        final String marker = matcher.group(1).toUpperCase();
        final long txId =
                matcher.group(2) != null ? Long.parseLong(matcher.group(2)) : UNDEFINED_TX_ID;
        final Instant ts = Instant.now();

        if ("BEGIN".equals(marker)) {
            currentTransactionId = txId;
            currentTransactionTimestamp = ts;
            processor.process(new TransactionMessage(ReplicationMessage.Operation.BEGIN, txId, ts));
        } else {
            final long commitTxId = currentTransactionId != null ? currentTransactionId : txId;
            final Instant commitTs =
                    currentTransactionTimestamp != null ? currentTransactionTimestamp : ts;
            processor.process(
                    new TransactionMessage(
                            ReplicationMessage.Operation.COMMIT, commitTxId, commitTs));
            currentTransactionId = null;
            currentTransactionTimestamp = null;
        }

        return true;
    }

    private void processTransactionJsonMessage(
            JsonNode root, String opType, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        final Long txIdFromJson = longValue(root, "tx_id", "xid", "transaction_id");
        final long txId = txIdFromJson != null ? txIdFromJson : UNDEFINED_TX_ID;
        final Instant ts = Instant.now();

        if ("BEGIN".equalsIgnoreCase(opType)) {
            currentTransactionId = txId;
            currentTransactionTimestamp = ts;
            processor.process(new TransactionMessage(ReplicationMessage.Operation.BEGIN, txId, ts));
        } else {
            final long commitTxId = currentTransactionId != null ? currentTransactionId : txId;
            final Instant commitTs =
                    currentTransactionTimestamp != null ? currentTransactionTimestamp : ts;
            processor.process(
                    new TransactionMessage(
                            ReplicationMessage.Operation.COMMIT, commitTxId, commitTs));
            currentTransactionId = null;
            currentTransactionTimestamp = null;
        }
    }

    private static Long longValue(JsonNode root, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode node = root.get(fieldName);
            if (node == null || node.isNull()) {
                continue;
            }
            if (node.isNumber()) {
                return node.asLong();
            }
            if (node.isTextual()) {
                try {
                    return Long.parseLong(node.asText());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return null;
    }

    private static String textValue(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asText();
    }

    private static ReplicationMessage.Operation parseOperation(String opType) {
        switch (opType.toUpperCase()) {
            case "INSERT":
                return ReplicationMessage.Operation.INSERT;
            case "UPDATE":
                return ReplicationMessage.Operation.UPDATE;
            case "DELETE":
                return ReplicationMessage.Operation.DELETE;
            default:
                return null;
        }
    }

    private static List<ReplicationMessage.Column> toColumns(
            TypeRegistry typeRegistry,
            JsonNode names,
            JsonNode types,
            JsonNode values,
            String originalMessage) {
        if (names == null || types == null || values == null) {
            return null;
        }
        if (!names.isArray() || !types.isArray() || !values.isArray()) {
            LOG.warn(
                    "Column fields are not arrays in mppdb_decoding message, skipping columns: {}",
                    originalMessage);
            return null;
        }
        if (names.size() != types.size() || names.size() != values.size()) {
            LOG.warn(
                    "Column arrays have different sizes in mppdb_decoding message, skipping columns: {}",
                    originalMessage);
            return null;
        }

        final List<ReplicationMessage.Column> columns = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            final String columnName = names.get(i).asText();
            final String fullType = types.get(i).asText();
            final JsonNode valueNode = values.get(i);
            final String rawValue =
                    valueNode == null || valueNode.isNull() ? null : valueNode.asText();

            final PostgresType columnType = typeRegistry.get(parseType(columnName, fullType));
            final MppdbColumnValue columnValue = new MppdbColumnValue(rawValue);

            columns.add(
                    new AbstractReplicationMessageColumn(
                            columnName, columnType, fullType, true, true) {
                        @Override
                        public Object getValue(
                                PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                            return ReplicationMessageColumnValueResolver.resolveValue(
                                    columnName,
                                    columnType,
                                    fullType,
                                    columnValue,
                                    connection,
                                    includeUnknownDatatypes,
                                    typeRegistry);
                        }

                        @Override
                        public String toString() {
                            return columnName + "(" + fullType + ")=" + rawValue;
                        }
                    });
        }
        return columns;
    }

    private static String parseType(String columnName, String typeWithModifiers) {
        Matcher matcher =
                AbstractReplicationMessageColumn.TypeMetadataImpl.TYPE_PATTERN.matcher(
                        typeWithModifiers);
        if (!matcher.matches()) {
            LOG.warn("Failed to parse columnType for {} '{}'", columnName, typeWithModifiers);
            return TypeRegistry.normalizeTypeName(typeWithModifiers);
        }
        String baseType = matcher.group("base").trim();
        final String suffix = matcher.group("suffix");
        if (suffix != null) {
            baseType += suffix;
        }
        baseType = TypeRegistry.normalizeTypeName(baseType);
        if (matcher.group("array") != null) {
            baseType = "_" + baseType;
        }
        return baseType;
    }

    private static String normalizeTableName(String tableName) {
        final String trimmed = tableName.trim();
        final int lastDot = trimmed.lastIndexOf('.');
        if (lastDot < 0) {
            return "\"" + trimmed + "\"";
        }
        final String schema = trimmed.substring(0, lastDot);
        final String table = trimmed.substring(lastDot + 1);
        return "\"" + schema + "\".\"" + table + "\"";
    }

    private static class MppdbDecodingReplicationMessage implements ReplicationMessage {
        private final Operation operation;
        private final Long transactionId;
        private final Instant commitTime;
        private final String table;
        private final List<Column> oldTuple;
        private final List<Column> newTuple;
        private final boolean lastEventForLsn;

        private MppdbDecodingReplicationMessage(
                Operation operation,
                Long transactionId,
                Instant commitTime,
                String table,
                List<Column> oldTuple,
                List<Column> newTuple,
                boolean lastEventForLsn) {
            this.operation = operation;
            this.transactionId = transactionId;
            this.commitTime = commitTime;
            this.table = table;
            this.oldTuple = oldTuple;
            this.newTuple = newTuple;
            this.lastEventForLsn = lastEventForLsn;
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public Instant getCommitTime() {
            return commitTime;
        }

        @Override
        public OptionalLong getTransactionId() {
            return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
        }

        @Override
        public String getTable() {
            return table;
        }

        @Override
        public List<Column> getOldTupleList() {
            return oldTuple;
        }

        @Override
        public List<Column> getNewTupleList() {
            return newTuple;
        }

        @Override
        public boolean hasTypeMetadata() {
            return true;
        }

        @Override
        public boolean isLastEventForLsn() {
            return lastEventForLsn;
        }
    }

    private static class MppdbColumnValue extends AbstractColumnValue<String> {

        private final String value;

        private MppdbColumnValue(String value) {
            this.value = value;
        }

        @Override
        public String getRawValue() {
            return value;
        }

        @Override
        public boolean isNull() {
            return value == null || "null".equalsIgnoreCase(value.trim());
        }

        @Override
        public String asString() {
            if (value == null) {
                return null;
            }
            String normalized = value.trim();
            if (normalized.length() >= 2
                    && normalized.charAt(0) == '\''
                    && normalized.charAt(normalized.length() - 1) == '\'') {
                normalized = normalized.substring(1, normalized.length() - 1);
                normalized = normalized.replace("''", "'");
            }
            return normalized;
        }

        @Override
        public Boolean asBoolean() {
            final String s = asString();
            if (s == null) {
                return null;
            }
            if ("t".equalsIgnoreCase(s) || "true".equalsIgnoreCase(s) || "1".equals(s)) {
                return true;
            }
            if ("f".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s) || "0".equals(s)) {
                return false;
            }
            return null;
        }

        @Override
        public Integer asInteger() {
            final String s = asString();
            return s != null ? Integer.valueOf(s) : null;
        }

        @Override
        public Long asLong() {
            final String s = asString();
            return s != null ? Long.valueOf(s) : null;
        }

        @Override
        public Float asFloat() {
            final String s = asString();
            return s != null ? Float.valueOf(s) : null;
        }

        @Override
        public Double asDouble() {
            final String s = asString();
            return s != null ? Double.valueOf(s) : null;
        }

        @Override
        public SpecialValueDecimal asDecimal() {
            final String s = asString();
            if (s == null) {
                return null;
            }
            try {
                return new SpecialValueDecimal(new BigDecimal(s));
            } catch (NumberFormatException e) {
                return SpecialValueDecimal.valueOf(s);
            }
        }

        @Override
        public byte[] asByteArray() {
            final String s = asString();
            if (s == null) {
                return null;
            }
            String normalized = s;
            if (normalized.startsWith("\\\\x")) {
                normalized = normalized.substring(3);
            } else if (normalized.startsWith("\\x")) {
                normalized = normalized.substring(2);
            } else if (normalized.startsWith("0x")) {
                normalized = normalized.substring(2);
            }
            return Strings.hexStringToByteArray(normalized);
        }
    }
}
