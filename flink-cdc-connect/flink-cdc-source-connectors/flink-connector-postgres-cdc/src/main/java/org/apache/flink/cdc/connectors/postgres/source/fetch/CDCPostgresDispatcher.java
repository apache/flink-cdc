/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresPartitionRoutingSchema;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/** Postgres Dispatcher for cdc source with watermark. */
public class CDCPostgresDispatcher extends PostgresEventDispatcher<TableId>
        implements WatermarkDispatcher {
    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;

    /**
     * Creates a new CDCPostgresDispatcher with partition routing support.
     *
     * <p>This dispatcher uses {@link PostgresPartitionRoutingSchema} to handle PostgreSQL partition
     * table routing, ensuring that events from partition child tables are correctly routed to their
     * parent tables. The data collection filter is obtained from the schema to maintain consistency
     * with the routing logic.
     *
     * @param connectorConfig the PostgreSQL connector configuration
     * @param topicSelector the topic selector for routing events to Kafka topics
     * @param schema the partition routing schema that handles partition table routing and provides
     *     consistent data collection filtering
     * @param queue the change event queue for buffering events
     * @param changeEventCreator the creator for change events
     * @param metadataProvider the provider for event metadata
     * @param heartbeatFactory the factory for creating heartbeat events
     * @param schemaNameAdjuster the adjuster for schema names
     */
    public CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            PostgresPartitionRoutingSchema schema,
            ChangeEventQueue queue,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                schema.getDataCollectionFilter(),
                changeEventCreator,
                metadataProvider,
                heartbeatFactory,
                schemaNameAdjuster);
        this.topic = topicSelector.getPrimaryTopic();
        this.queue = queue;
    }

    @Override
    public void dispatchWatermarkEvent(
            Map<String, ?> sourcePartition,
            SourceSplitBase sourceSplit,
            Offset watermark,
            WatermarkKind watermarkKind)
            throws InterruptedException {
        SourceRecord sourceRecord =
                WatermarkEvent.create(
                        sourcePartition, topic, sourceSplit.splitId(), watermarkKind, watermark);
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }
}
