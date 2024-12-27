package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class MySqlPipelineSourceRecordEmitterSupplier
        implements MySqlSource.RecordEmitterSupplier<Event> {

    private MySqlEventDeserializer deserializer;
    private boolean isBatchMode;

    public MySqlPipelineSourceRecordEmitterSupplier(
            MySqlEventDeserializer deserializer, boolean isBatchMode) {
        this.deserializer = deserializer;
        this.isBatchMode = isBatchMode;
    }

    @Override
    public RecordEmitter<SourceRecords, Event, MySqlSplitState> get(
            MySqlSourceReaderMetrics metrics, MySqlSourceConfig sourceConfig) {
        return new MySqlPipelineRecordEmitter(deserializer, metrics, sourceConfig, isBatchMode);
    }
}
