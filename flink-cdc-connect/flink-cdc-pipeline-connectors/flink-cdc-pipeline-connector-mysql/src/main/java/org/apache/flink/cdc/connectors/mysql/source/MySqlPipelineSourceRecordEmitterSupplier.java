package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.List;

public class MySqlPipelineSourceRecordEmitterSupplier
        implements MySqlSource.RecordEmitterSupplier<Event> {

    private MySqlEventDeserializer deserializer;
    private boolean isBatchMode;
    private List<RouteRule> routeRules;

    public MySqlPipelineSourceRecordEmitterSupplier(
            MySqlEventDeserializer deserializer, boolean isBatchMode, List<RouteRule> routeRules) {
        this.deserializer = deserializer;
        this.isBatchMode = isBatchMode;
        this.routeRules = routeRules;
    }

    @Override
    public RecordEmitter<SourceRecords, Event, MySqlSplitState> get(
            MySqlSourceReaderMetrics metrics, MySqlSourceConfig sourceConfig) {
        MySqlPipelineRecordEmitter mySqlPipelineRecordEmitter =
                new MySqlPipelineRecordEmitter(deserializer, metrics, sourceConfig);
        if (isBatchMode) {
            mySqlPipelineRecordEmitter.mergeCreateTableEventByRoutes(routeRules);
        }
        return mySqlPipelineRecordEmitter;
    }
}
