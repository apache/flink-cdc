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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.flink.cdc.connectors.elasticsearch.serializer.ElasticsearchEventSerializer;
import org.apache.flink.cdc.connectors.elasticsearch.v2.Elasticsearch8AsyncSinkBuilder;

import org.apache.http.HttpHost;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * A {@link DataSink} implementation for Elasticsearch connector.
 *
 * @param <InputT> The input type of the sink.
 */
public class ElasticsearchDataSink<InputT> implements DataSink, Serializable {

    private static final long serialVersionUID = 1L;

    /** The Elasticsearch sink options. */
    private final ElasticsearchSinkOptions elasticsearchOptions;

    /** The time zone ID for handling time-related operations. */
    private final ZoneId zoneId;

    /**
     * Constructs an ElasticsearchDataSink with the given options and time zone.
     *
     * @param elasticsearchOptions The Elasticsearch sink options.
     * @param zoneId The time zone ID for handling time-related operations.
     */
    public ElasticsearchDataSink(ElasticsearchSinkOptions elasticsearchOptions, ZoneId zoneId) {
        this.elasticsearchOptions = elasticsearchOptions;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(
                new Elasticsearch8AsyncSinkBuilder<Event>()
                        .setHosts(elasticsearchOptions.getHosts().toArray(new HttpHost[0]))
                        .setElementConverter(
                                new ElasticsearchEventSerializer(ZoneId.systemDefault()))
                        .setMaxBatchSize(elasticsearchOptions.getMaxBatchSize())
                        .setMaxInFlightRequests(elasticsearchOptions.getMaxInFlightRequests())
                        .setMaxBufferedRequests(elasticsearchOptions.getMaxBufferedRequests())
                        .setMaxBatchSizeInBytes(elasticsearchOptions.getMaxBatchSizeInBytes())
                        .setMaxTimeInBufferMS(elasticsearchOptions.getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(elasticsearchOptions.getMaxRecordSizeInBytes())
                        .build());
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        // Currently, no metadata application is needed for Elasticsearch
        return schemaChangeEvent -> {};
    }
}
