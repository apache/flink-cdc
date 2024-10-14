package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergEventSink;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergRecordSerializer;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class IcebergDataSink implements DataSink, Serializable {

    // options for creating Iceberg catalog.
    private final Map<String, String> catalogOptions;

    // options for creating Iceberg table.
    private final Map<String, String> tableOptions;

    private final String commitUser;

    private final Map<TableId, List<String>> partitionMaps;

    private final IcebergRecordSerializer<Event> serializer;

    private final ZoneId zoneId;

    public final String schemaOperatorUid;

    public IcebergDataSink(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            String commitUser,
            Map<TableId, List<String>> partitionMaps,
            IcebergRecordSerializer<Event> serializer,
            ZoneId zoneId,
            String schemaOperatorUid) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.commitUser = commitUser;
        this.partitionMaps = partitionMaps;
        this.serializer = serializer;
        this.zoneId = zoneId;
        this.schemaOperatorUid = schemaOperatorUid;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        IcebergEventSink icebergEventSink =
                new IcebergEventSink(
                        tableOptions, commitUser, serializer, schemaOperatorUid, zoneId);
        return FlinkSinkProvider.of(icebergEventSink);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new IcebergMetadataApplier(catalogOptions, tableOptions, partitionMaps);
    }

    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        // TODO getDataChangeEventHashFunctionProvider if use
        return DataSink.super.getDataChangeEventHashFunctionProvider();
    }

    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider(
            int parallelism) {
        return DataSink.super.getDataChangeEventHashFunctionProvider(parallelism);
    }
}
