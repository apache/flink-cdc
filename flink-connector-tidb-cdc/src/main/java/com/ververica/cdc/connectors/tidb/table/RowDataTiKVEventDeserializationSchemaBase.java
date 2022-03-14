package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of deserialization schema from TiKV RowValue (Snapshot or Change Event) to Flink
 * Table/SQL internal data structure {@link RowData}.
 */
public class RowDataTiKVEventDeserializationSchemaBase implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Whether the deserializer needs to handle metadata columns. */
    private final boolean hasMetadata;

    /**
     * A wrapped output collector which is used to append metadata columns after physical columns.
     */
    private final TiKVAppendMetadataCollector appendMetadataCollector;

    public RowDataTiKVEventDeserializationSchemaBase(TiKVMetadataConverter[] metadataConverters) {
        this.hasMetadata = checkNotNull(metadataConverters).length > 0;
        this.appendMetadataCollector = new TiKVAppendMetadataCollector(metadataConverters);
    }

    public void emit(
            TiKVMetadataConverter.TiKVRowValue inRecord,
            RowData physicalRow,
            Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }

        appendMetadataCollector.row = inRecord;
        appendMetadataCollector.outputCollector = collector;
        appendMetadataCollector.collect(physicalRow);
    }
}
