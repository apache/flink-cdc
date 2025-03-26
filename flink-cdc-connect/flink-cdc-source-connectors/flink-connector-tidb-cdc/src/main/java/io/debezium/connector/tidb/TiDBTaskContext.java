package io.debezium.connector.tidb;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class TiDBTaskContext extends CdcSourceTaskContext {
    private final TiDBDatabaseSchema schema;
    private final TopicSelector<TableId> topicSelector;

    public TiDBTaskContext(TiDBConnectorConfig config, TiDBDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.schema = schema;
        topicSelector = TidbTopicSelector.defaultSelector(config);
    }

    public TiDBDatabaseSchema getSchema() {
        return schema;
    }

    public TopicSelector<TableId> getTopicSelector() {
        return topicSelector;
    }
}
