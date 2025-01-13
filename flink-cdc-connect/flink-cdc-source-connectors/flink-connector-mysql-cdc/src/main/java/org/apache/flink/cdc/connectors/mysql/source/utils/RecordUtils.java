/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WatermarkKind;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SIGNAL_EVENT_VALUE_SCHEMA_NAME;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SPLIT_ID_KEY;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WATERMARK_KIND;

/** Utility class to deal record. */
public class RecordUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

    private RecordUtils() {}

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";
    public static final String SCHEMA_HEARTBEAT_EVENT_KEY_NAME =
            "io.debezium.connector.common.Heartbeat";
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    /** Converts a {@link ResultSet} row to an array of Objects. */
    public static Object[] rowToArray(ResultSet rs, int size) throws SQLException {
        final Object[] row = new Object[size];
        for (int i = 0; i < size; i++) {
            row[i] = rs.getObject(i + 1);
        }
        return row;
    }

    public static Struct getStructContainsChunkKey(SourceRecord record) {
        // Use chunk key in the after struct for insert or
        // the before struct for delete/update
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            return value.getStruct(Envelope.FieldName.AFTER);
        } else {
            return value.getStruct(Envelope.FieldName.BEFORE);
        }
    }

    /** upsert binlog events to snapshot events collection. */
    public static void upsertBinlog(
            Map<Struct, List<SourceRecord>> snapshotRecords,
            SourceRecord binlogRecord,
            RowType splitBoundaryType,
            SchemaNameAdjuster nameAdjuster,
            Object[] splitStart,
            Object[] splitEnd) {
        if (isDataChangeRecord(binlogRecord)) {
            Struct value = (Struct) binlogRecord.value();
            if (value != null) {
                Struct chunkKeyStruct = getStructContainsChunkKey(binlogRecord);
                if (splitKeyRangeContains(
                        getSplitKey(splitBoundaryType, nameAdjuster, chunkKeyStruct),
                        splitStart,
                        splitEnd)) {
                    boolean hasPrimaryKey = binlogRecord.key() != null;
                    Envelope.Operation operation =
                            Envelope.Operation.forCode(
                                    value.getString(Envelope.FieldName.OPERATION));
                    switch (operation) {
                        case CREATE:
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey
                                            ? (Struct) binlogRecord.key()
                                            : createReadOpValue(
                                                    binlogRecord, Envelope.FieldName.AFTER),
                                    false);
                            break;
                        case UPDATE:
                            Struct structFromAfter =
                                    createReadOpValue(binlogRecord, Envelope.FieldName.AFTER);
                            if (!hasPrimaryKey) {
                                upsertBinlog(
                                        snapshotRecords,
                                        binlogRecord,
                                        createReadOpValue(binlogRecord, Envelope.FieldName.BEFORE),
                                        true);
                                if (!splitKeyRangeContains(
                                        getSplitKey(
                                                splitBoundaryType, nameAdjuster, structFromAfter),
                                        splitStart,
                                        splitEnd)) {
                                    LOG.warn(
                                            "The updated chunk key is out of the split range. Cannot provide exactly-once semantics.");
                                }
                            }
                            // If the chunk key changed, we still send here
                            // This will cause the at-least-once semantics
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey ? (Struct) binlogRecord.key() : structFromAfter,
                                    false);
                            break;
                        case DELETE:
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey
                                            ? (Struct) binlogRecord.key()
                                            : createReadOpValue(
                                                    binlogRecord, Envelope.FieldName.BEFORE),
                                    true);
                            break;
                        case READ:
                            throw new IllegalStateException(
                                    String.format(
                                            "Binlog record shouldn't use READ operation, the the record is %s.",
                                            binlogRecord));
                    }
                }
            }
        }
    }

    private static void upsertBinlog(
            Map<Struct, List<SourceRecord>> snapshotRecords,
            SourceRecord binlogRecord,
            Struct keyStruct,
            boolean isDelete) {
        boolean hasPrimaryKey = binlogRecord.key() != null;
        List<SourceRecord> records = snapshotRecords.get(keyStruct);
        if (isDelete) {
            if (records == null || records.isEmpty()) {
                LOG.error(
                        "Deleting a record which is not in its split for tables without primary keys. This may happen when the chunk key column is updated in another snapshot split.");
            } else if (hasPrimaryKey) {
                snapshotRecords.remove(keyStruct);
            } else {
                snapshotRecords.get(keyStruct).remove(0);
            }
        } else {
            SourceRecord record =
                    new SourceRecord(
                            binlogRecord.sourcePartition(),
                            binlogRecord.sourceOffset(),
                            binlogRecord.topic(),
                            binlogRecord.kafkaPartition(),
                            binlogRecord.keySchema(),
                            binlogRecord.key(),
                            binlogRecord.valueSchema(),
                            createReadOpValue(binlogRecord, Envelope.FieldName.AFTER));
            if (hasPrimaryKey) {
                snapshotRecords.put(keyStruct, Collections.singletonList(record));
            } else {
                if (records == null) {
                    snapshotRecords.put(keyStruct, new LinkedList<>());
                    records = snapshotRecords.get(keyStruct);
                }
                records.add(record);
            }
        }
    }

    private static Struct createReadOpValue(SourceRecord binlogRecord, String beforeOrAfter) {
        Struct value = (Struct) binlogRecord.value();

        Envelope envelope = Envelope.fromSchema(binlogRecord.valueSchema());
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Struct targetStruct = value.getStruct(beforeOrAfter);
        Instant fetchTs = Instant.ofEpochMilli((Long) source.get(Envelope.FieldName.TIMESTAMP));
        return envelope.read(targetStruct, source, fetchTs);
    }

    /**
     * Format message timestamp(source.ts_ms) value to 0L for all records read in snapshot phase.
     */
    public static List<SourceRecord> formatMessageTimestamp(
            Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .map(
                        record -> {
                            Envelope envelope = Envelope.fromSchema(record.valueSchema());
                            Struct value = (Struct) record.value();
                            Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                            source.put(Envelope.FieldName.TIMESTAMP, 0L);
                            // extend the fetch timestamp(ts_ms)
                            Instant fetchTs =
                                    Instant.ofEpochMilli(
                                            value.getInt64(Envelope.FieldName.TIMESTAMP));
                            SourceRecord sourceRecord =
                                    new SourceRecord(
                                            record.sourcePartition(),
                                            record.sourceOffset(),
                                            record.topic(),
                                            record.kafkaPartition(),
                                            record.keySchema(),
                                            record.key(),
                                            record.valueSchema(),
                                            envelope.read(updateAfter, source, fetchTs));
                            return sourceRecord;
                        })
                .collect(Collectors.toList());
    }

    public static boolean isWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent();
    }

    public static boolean isLowWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.LOW) {
            return true;
        }
        return false;
    }

    public static boolean isHighWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.HIGH) {
            return true;
        }
        return false;
    }

    public static boolean isEndWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.BINLOG_END) {
            return true;
        }
        return false;
    }

    public static BinlogOffset getWatermark(SourceRecord watermarkEvent) {
        return getBinlogPosition(watermarkEvent.sourceOffset());
    }

    /**
     * Return the timestamp when the change event is produced in MySQL.
     *
     * <p>The field `source.ts_ms` in {@link SourceRecord} data struct is the time when the change
     * event is operated in MySQL.
     */
    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }

        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /**
     * Return the timestamp when the change event is fetched in {@link DebeziumReader}.
     *
     * <p>The field `ts_ms` in {@link SourceRecord} data struct is the time when the record fetched
     * by debezium reader, use it as the process time in Source.
     */
    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }
        return value.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    public static boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        if (keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name())) {
            return true;
        }
        return false;
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
    }

    /**
     * Return the finished snapshot split information.
     *
     * @return [splitId, splitStart, splitEnd, highWatermark], the information will be used to
     *     filter binlog events when read binlog of table.
     */
    public static FinishedSnapshotSplitInfo getSnapshotSplitInfo(
            MySqlSnapshotSplit split, SourceRecord highWatermark) {
        Struct value = (Struct) highWatermark.value();
        String splitId = value.getString(SPLIT_ID_KEY);
        return new FinishedSnapshotSplitInfo(
                split.getTableId(),
                splitId,
                split.getSplitStart(),
                split.getSplitEnd(),
                getBinlogPosition(highWatermark.sourceOffset()));
    }

    /** Returns the start offset of the binlog split. */
    public static BinlogOffset getStartingOffsetOfBinlogSplit(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits) {
        BinlogOffset startOffset =
                finishedSnapshotSplits.isEmpty()
                        ? BinlogOffset.ofEarliest()
                        : finishedSnapshotSplits.get(0).getHighWatermark();
        for (FinishedSnapshotSplitInfo finishedSnapshotSplit : finishedSnapshotSplits) {
            if (finishedSnapshotSplit.getHighWatermark().isBefore(startOffset)) {
                startOffset = finishedSnapshotSplit.getHighWatermark();
            }
        }
        return startOffset;
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(dbName, null, tableName);
    }

    public static SourceRecord setTableId(
            SourceRecord dataRecord, TableId originalTableId, TableId tableId) {
        Struct value = (Struct) dataRecord.value();
        Document historyRecordDocument;
        try {
            historyRecordDocument = getHistoryRecord(dataRecord).document();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HistoryRecord newHistoryRecord =
                new HistoryRecord(
                        historyRecordDocument.set(
                                HistoryRecord.Fields.DDL_STATEMENTS,
                                historyRecordDocument
                                        .get(HistoryRecord.Fields.DDL_STATEMENTS)
                                        .asString()
                                        .replace(originalTableId.table(), tableId.table())));

        Struct newSource =
                value.getStruct(Envelope.FieldName.SOURCE)
                        .put(DATABASE_NAME_KEY, tableId.catalog())
                        .put(TABLE_NAME_KEY, tableId.table());
        return dataRecord.newRecord(
                dataRecord.topic(),
                dataRecord.kafkaPartition(),
                dataRecord.keySchema(),
                dataRecord.key(),
                dataRecord.valueSchema(),
                value.put(Envelope.FieldName.SOURCE, newSource)
                        .put(HISTORY_RECORD_FIELD, newHistoryRecord.toString()),
                dataRecord.timestamp(),
                dataRecord.headers());
    }

    public static boolean isTableChangeRecord(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String tableName = source.getString(TABLE_NAME_KEY);
        return !StringUtils.isNullOrWhitespaceOnly(tableName);
    }

    public static Object[] getSplitKey(
            RowType splitBoundaryType, SchemaNameAdjuster nameAdjuster, Struct target) {
        // the split key field contains single field now
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames().get(0));
        return new Object[] {target.get(splitFieldName)};
    }

    public static BinlogOffset getBinlogPosition(SourceRecord dataRecord) {
        return getBinlogPosition(dataRecord.sourceOffset());
    }

    public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return BinlogOffset.builder().setOffsetMap(offsetStrMap).build();
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        // for all range
        if (splitKeyStart == null && splitKeyEnd == null) {
            return true;
        }
        // first split
        if (splitKeyStart == null) {
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                    && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0);
        }
        // last split
        else if (splitKeyEnd == null) {
            int[] lowerBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
            }
            return Arrays.stream(lowerBoundRes).allMatch(value -> value >= 0);
        }
        // other split
        else {
            int[] lowerBoundRes = new int[key.length];
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(lowerBoundRes).anyMatch(value -> value >= 0)
                    && (Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                            && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0));
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
            return ((Comparable) o1).compareTo(o2);
        } else if (isNumericObject(o1) && isNumericObject(o2)) {
            return toBigDecimal(o1).compareTo(toBigDecimal(o2));
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    }

    private static boolean isNumericObject(Object obj) {
        return obj instanceof Byte
                || obj instanceof Short
                || obj instanceof Integer
                || obj instanceof Long
                || obj instanceof Float
                || obj instanceof Double
                || obj instanceof BigInteger
                || obj instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object numericObj) {
        return new BigDecimal(numericObj.toString());
    }

    public static HistoryRecord getHistoryRecord(SourceRecord schemaRecord) throws IOException {
        Struct value = (Struct) schemaRecord.value();
        String historyRecordStr = value.getString(HISTORY_RECORD_FIELD);
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    private static Optional<WatermarkKind> getWatermarkKind(SourceRecord record) {
        if (record.valueSchema() != null
                && SIGNAL_EVENT_VALUE_SCHEMA_NAME.equals(record.valueSchema().name())) {
            Struct value = (Struct) record.value();
            return Optional.of(WatermarkKind.valueOf(value.getString(WATERMARK_KIND)));
        }
        return Optional.empty();
    }

    /**
     * This utility method checks if given source record is a gh-ost/pt-osc initiated schema change
     * event by checking the "alter" ddl.
     */
    public static boolean isOnLineSchemaChangeEvent(SourceRecord record) {
        if (!isSchemaChangeEvent(record)) {
            return false;
        }
        Struct value = (Struct) record.value();
        ObjectMapper mapper = new ObjectMapper();
        try {
            // There will be these schema change events generated in total during one transaction.
            //
            // gh-ost:
            // DROP TABLE IF EXISTS `db`.`_tb1_gho`
            // DROP TABLE IF EXISTS `db`.`_tb1_del`
            // DROP TABLE IF EXISTS `db`.`_tb1_ghc`
            // create /* gh-ost */ table `db`.`_tb1_ghc` ...
            // create /* gh-ost */ table `db`.`_tb1_gho` like `db`.`tb1`
            // alter /* gh-ost */ table `db`.`_tb1_gho` add column c varchar(255)
            // create /* gh-ost */ table `db`.`_tb1_del` ...
            // DROP TABLE IF EXISTS `db`.`_tb1_del`
            // rename /* gh-ost */ table `db`.`tb1` to `db`.`_tb1_del`
            // rename /* gh-ost */ table `db`.`_tb1_gho` to `db`.`tb1`
            // DROP TABLE IF EXISTS `db`.`_tb1_ghc`
            // DROP TABLE IF EXISTS `db`.`_tb1_del`
            //
            // pt-osc:
            // CREATE TABLE `db`.`_test_tb1_new`
            // ALTER TABLE `db`.`_test_tb1_new` add column c varchar(50)
            // CREATE TRIGGER `pt_osc_db_test_tb1_del`...
            // CREATE TRIGGER `pt_osc_db_test_tb1_upd`...
            // CREATE TRIGGER `pt_osc_db_test_tb1_ins`...
            // ANALYZE TABLE `db`.`_test_tb1_new` /* pt-online-schema-change */
            // RENAME TABLE `db`.`test_tb1` TO `db`.`_test_tb1_old`, `db`.`_test_tb1_new` TO
            // `db`.`test_tb1`
            // DROP TABLE IF EXISTS `_test_tb1_old` /* generated by server */
            // DROP TRIGGER IF EXISTS `db`.`pt_osc_db_test_tb1_del`
            // DROP TRIGGER IF EXISTS `db`.`pt_osc_db_test_tb1_upd`
            // DROP TRIGGER IF EXISTS `db`.`pt_osc_db_test_tb1_ins`
            //
            // Among all these, we only need the "ALTER" one that happens on the `_gho`/`_new`
            // table.
            String ddl =
                    mapper.readTree(value.getString(HISTORY_RECORD_FIELD))
                            .get(HistoryRecord.Fields.DDL_STATEMENTS)
                            .asText()
                            .toLowerCase();
            if (ddl.startsWith("alter")) {
                String tableName =
                        value.getStruct(Envelope.FieldName.SOURCE).getString(TABLE_NAME_KEY);
                return OSC_TABLE_ID_PATTERN.matcher(tableName).matches();
            }

            return false;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    private static final Pattern OSC_TABLE_ID_PATTERN = Pattern.compile("^_(.*)_(gho|new)$");

    /** This utility method peels out gh-ost/pt-osc mangled tableId to the original one. */
    public static TableId peelTableId(TableId tableId) {
        Matcher matchingResult = OSC_TABLE_ID_PATTERN.matcher(tableId.table());
        if (matchingResult.matches()) {
            return new TableId(tableId.catalog(), tableId.schema(), matchingResult.group(1));
        }
        return tableId;
    }
}
