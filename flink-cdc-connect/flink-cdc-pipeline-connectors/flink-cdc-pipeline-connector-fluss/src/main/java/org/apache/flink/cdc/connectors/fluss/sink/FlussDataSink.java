package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.fluss.config.FlussSinkOptions;

import com.alibaba.fluss.flink.sink.FlussSink;

import java.time.ZoneId;

public class FlussDataSink implements DataSink {
    private final FlussSinkOptions flssOptions;

    private final ZoneId zoneId;

    public FlussDataSink(FlussSinkOptions flssOptions, ZoneId zoneId) {
        this.flssOptions = flssOptions;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(
                FlussSink.<Event>builder()
                        .setBootstrapServers(flssOptions.getBootstrapServers())
                        .setDatabase(flssOptions.getDatabase())
                        .setTable(flssOptions.getTable())
                        .setSerializationSchema(new FlussEventSerializationSchema(zoneId))
                        .setOptions(flssOptions.getOptions())
                        .build());
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return null;
    }
}
