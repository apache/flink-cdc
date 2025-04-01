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

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/** Postgres Dispatcher for cdc source with watermark. */
public class CDCPostgresDispatcher extends PostgresEventDispatcher<TableId>
        implements WatermarkDispatcher {
    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            DatabaseSchema schema,
            ChangeEventQueue queue,
            DataCollectionFilters.DataCollectionFilter filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
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
