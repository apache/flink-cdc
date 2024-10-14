package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;

import java.time.ZoneId;
import java.util.List;

public class TableSchemaInfo {

    private final Schema schema;

    private final List<RecordData.FieldGetter> fieldGetters;

    public TableSchemaInfo(Schema schema, ZoneId zoneId) {
        this.schema = schema;
        this.fieldGetters = IcebergWriterHelper.createFieldGetters(schema, zoneId);
    }

    public Schema getSchema() {
        return schema;
    }

    public List<RecordData.FieldGetter> getFieldGetters() {
        return fieldGetters;
    }
}
