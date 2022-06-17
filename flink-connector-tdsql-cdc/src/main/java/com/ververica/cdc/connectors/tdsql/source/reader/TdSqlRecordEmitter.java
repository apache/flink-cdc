package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.tdsql.source.split.TdSqlSplitState;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * The {@link RecordEmitter} implementation for {@link TdSqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public class TdSqlRecordEmitter<T> implements RecordEmitter<SourceRecord, T, TdSqlSplitState> {
    private final MySqlRecordEmitter<T> mySqlRecordEmitter;

    public TdSqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges) {
        this.mySqlRecordEmitter =
                new MySqlRecordEmitter<>(
                        debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges);
    }

    @Override
    public void emitRecord(SourceRecord element, SourceOutput<T> output, TdSqlSplitState splitState)
            throws Exception {
        this.mySqlRecordEmitter.emitRecord(element, output, splitState.mySqlSplitState());
    }
}
