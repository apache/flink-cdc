package io.debezium.connector.tidb;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

import static org.apache.flink.cdc.connectors.tidb.source.offset.TiDBSourceInfo.COMMIT_VERSION_KEY;

public class TiDBEventMetadataProvider implements EventMetadataProvider {
    @Override
    public Instant getEventTimestamp(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (sourceInfo == null) {
            return null;
        }
        final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return Collect.hashMapOf(COMMIT_VERSION_KEY, sourceInfo.getString(COMMIT_VERSION_KEY));
    }

    @Override
    public String getTransactionId(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return ((MySqlOffsetContext) offset).getTransactionId();
    }
}
