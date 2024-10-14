package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.ZoneId;
import java.util.Map;

public class IcebergEventSink extends IcebergSink implements SupportsPreWriteTopology<Event> {

    public final String schemaOperatorUid;

    public final ZoneId zoneId;

    public IcebergEventSink(
            Map<String, String> catalogOptions,
            String commitUser,
            IcebergRecordSerializer<Event> serializer,
            String schemaOperatorUid,
            ZoneId zoneId) {
        super(catalogOptions, commitUser, serializer);
        this.schemaOperatorUid = schemaOperatorUid;
        this.zoneId = zoneId;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        return dataStream;
    }
}
