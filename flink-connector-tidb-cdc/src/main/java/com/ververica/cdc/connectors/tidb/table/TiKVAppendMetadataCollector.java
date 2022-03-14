package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/** Emits a row with physical fields and metadata fields. */
public class TiKVAppendMetadataCollector implements Collector<RowData>, Serializable {
    private static final long serialVersionUID = 1L;

    private final TiKVMetadataConverter[] metadataConverters;

    public transient TiKVMetadataConverter.TiKVRowValue row;
    public transient Collector<RowData> outputCollector;

    public TiKVAppendMetadataCollector(TiKVMetadataConverter[] metadataConverters) {
        this.metadataConverters = metadataConverters;
    }

    @Override
    public void collect(RowData physicalRow) {
        GenericRowData metaRow = new GenericRowData(metadataConverters.length);
        for (int i = 0; i < metadataConverters.length; i++) {
            Object meta = metadataConverters[i].read(row);
            metaRow.setField(i, meta);
        }
        RowData outRow = new JoinedRowData(physicalRow.getRowKind(), physicalRow, metaRow);
        outputCollector.collect(outRow);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
