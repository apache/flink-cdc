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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner}. */
public class PrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent> {

    private final Options catalogOptions;

    private final int downStreamParallelism;

    private final int subTaskId;

    private Catalog catalog;

    private PaimonRecordSerializer<Event> serializer;

    private Map<Identifier, RowKeyExtractor> extractorMap;

    public PrePartitionOperator(int downStreamParallelism, Options catalogOptions) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.catalogOptions = catalogOptions;
        this.downStreamParallelism = downStreamParallelism;
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void open() throws Exception {
        super.open();
        extractorMap = new HashMap<>();
        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        serializer = new PaimonRecordEventSerializer(ZoneId.systemDefault());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        if (event instanceof FlushEvent) {
            output.collect(new StreamRecord<>(new PartitioningEvent(event, subTaskId)));
        } else if (event instanceof SchemaChangeEvent) {
            serializer.serialize(event);
            output.collect(new StreamRecord<>(new PartitioningEvent(event, subTaskId)));
        } else if (event instanceof DataChangeEvent) {
            PaimonEvent paimonEvent = serializer.serialize(event);
            RowKeyExtractor extractor =
                    extractorMap.computeIfAbsent(
                            paimonEvent.getTableId(),
                            tableId -> {
                                try {
                                    return ((AbstractFileStoreTable) catalog.getTable(tableId))
                                            .createRowKeyExtractor();
                                } catch (Catalog.TableNotExistException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            extractor.setRecord(paimonEvent.getGenericRow());
            int bucket = extractor.bucket();
            // emit Event of the same bucket to the same subtask.
            output.collect(
                    new StreamRecord<>(
                            new PartitioningEvent(event, bucket % downStreamParallelism)));
        } else {
            throw new IllegalArgumentException("Unsupported event");
        }
    }
}
