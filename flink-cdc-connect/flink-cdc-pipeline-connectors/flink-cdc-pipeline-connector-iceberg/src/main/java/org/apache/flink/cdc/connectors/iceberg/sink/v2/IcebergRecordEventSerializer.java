package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

public class IcebergRecordEventSerializer implements IcebergRecordSerializer<Event> {

    // maintain the latest schema of tableId.
    private final Map<TableId, TableSchemaInfo> schemaMaps;

    // ZoneId for converting relevant type.
    private final ZoneId zoneId;

    public IcebergRecordEventSerializer(Map<TableId, TableSchemaInfo> schemaMaps, ZoneId zoneId) {
        this.schemaMaps = schemaMaps;
        this.zoneId = zoneId;
    }

    @Override
    public IcebergEvent serialize(Event event) throws IOException {
        TableIdentifier tableId = TableIdentifier.of(((ChangeEvent) event).tableId().toString());
        if (event instanceof SchemaChangeEvent) {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemaMaps.put(
                        createTableEvent.tableId(),
                        new TableSchemaInfo(createTableEvent.getSchema(), zoneId));
            } else {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                schemaMaps.put(
                        schemaChangeEvent.tableId(),
                        new TableSchemaInfo(
                                SchemaUtils.applySchemaChangeEvent(
                                        schemaMaps.get(schemaChangeEvent.tableId()).getSchema(),
                                        schemaChangeEvent),
                                zoneId));
            }
            return new IcebergEvent(tableId, null, true);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            GenericRecord genericRecord =
                    IcebergWriterHelper.convertEventToGenericRow(
                            dataChangeEvent,
                            schemaMaps.get(dataChangeEvent.tableId()).getFieldGetters());
            return new IcebergEvent(tableId, genericRecord, false);
        } else {
            throw new IllegalArgumentException(
                    "failed to convert Input into PaimonEvent, unsupported event: " + event);
        }
    }
}
