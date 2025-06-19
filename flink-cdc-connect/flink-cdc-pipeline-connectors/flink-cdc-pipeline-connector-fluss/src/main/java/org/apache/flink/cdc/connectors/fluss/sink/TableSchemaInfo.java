package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.fluss.utils.FlussUtils;

import java.time.ZoneId;

public class TableSchemaInfo {
    public final Schema schema;

    public final RecordData.FieldGetter[] fieldGetters;

    public TableSchemaInfo(Schema schema, ZoneId zoneId) {
        this.schema = schema;
        fieldGetters = new RecordData.FieldGetter[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters[i] =
                    FlussUtils.createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId);
        }
    }
}
