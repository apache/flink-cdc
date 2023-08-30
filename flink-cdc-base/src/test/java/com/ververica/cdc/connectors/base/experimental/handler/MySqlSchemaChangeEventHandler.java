package com.ververica.cdc.connectors.base.experimental.handler;

import com.ververica.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;
import io.debezium.schema.SchemaChangeEvent;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.BINLOG_FILENAME_OFFSET_KEY;
import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.BINLOG_POSITION_OFFSET_KEY;
import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.SERVER_ID_KEY;

/** MySqlSchemaChangeEventHandler to deal MySql's schema change event. */
public class MySqlSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        Map<String, Object> source = new HashMap<>();
        Struct sourceInfo = event.getSource();
        String fileName = sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY);
        Long pos = sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY);
        Long serverId = sourceInfo.getInt64(SERVER_ID_KEY);
        source.put(SERVER_ID_KEY, serverId);
        source.put(BINLOG_FILENAME_OFFSET_KEY, fileName);
        source.put(BINLOG_POSITION_OFFSET_KEY, pos);
        return source;
    }
}
