package com.ververica.cdc.debezium.utils;

import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Utilities to {@link SourceRecord}.
 */
public class SourceRecordUtil {

    private SourceRecordUtil() {
    }

    /**
     * Return {@link SourceRecord} which correct the record time zone
     */
    public static SourceRecord correctTimeZoneSourceRecord(SourceRecord record, int timeZoneOffset) {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Struct after = extractAfterStruct(value, valueSchema, timeZoneOffset);
            value.put(Envelope.FieldName.AFTER, after);
        } else if (op == Envelope.Operation.DELETE) {
            Struct before = extractBeforeStruct(value, valueSchema, timeZoneOffset);
            value.put(Envelope.FieldName.BEFORE, before);
        } else {
            Struct before = extractBeforeStruct(value, valueSchema, timeZoneOffset);
            value.put(Envelope.FieldName.BEFORE, before);
            Struct after = extractAfterStruct(value, valueSchema, timeZoneOffset);
            value.put(Envelope.FieldName.AFTER, after);
        }

        SourceRecord sourceRecord = new SourceRecord(record.sourcePartition(), record.sourceOffset(), record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, value);
        return sourceRecord;
    }

    private static Struct extractBeforeStruct(Struct value, Schema valueSchema, int timeZoneOffset) {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct beforeStruct = value.getStruct(Envelope.FieldName.BEFORE);
        for (Field field : beforeSchema.fields()) {
            beforeStruct = convertTimestamp(beforeStruct, field, timeZoneOffset);
        }
        return beforeStruct;
    }

    private static Struct extractAfterStruct(Struct value, Schema valueSchema, int timeZoneOffset) {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct afterStruct = value.getStruct(Envelope.FieldName.AFTER);
        for (Field field : afterSchema.fields()) {
            afterStruct = convertTimestamp(afterStruct, field, timeZoneOffset);
        }
        return afterStruct;
    }

    private static Struct convertTimestamp(Struct struct, Field field, int timeZoneOffset) {
        if (struct.get(field) != null) {
            //DATETIME TYPE
            if (Timestamp.SCHEMA_NAME.equals(field.schema().name())) {
                struct.put(field, (Long) struct.get(field) - ZoneOffset.ofHours(timeZoneOffset).getTotalSeconds() * 1000);
            }
            //TIMESTAMP TYPE
            if (ZonedTimestamp.SCHEMA_NAME.equals(field.schema().name())) {
                if (struct.get(field) instanceof String) {
                    LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(struct.get(field), ZoneOffset.UTC);
                    String timeStamp = localDateTime.toInstant(ZoneOffset.ofHours(-timeZoneOffset)).toString();
                    struct.put(field, timeStamp);
                }
            }
        }
        return struct;
    }
}
