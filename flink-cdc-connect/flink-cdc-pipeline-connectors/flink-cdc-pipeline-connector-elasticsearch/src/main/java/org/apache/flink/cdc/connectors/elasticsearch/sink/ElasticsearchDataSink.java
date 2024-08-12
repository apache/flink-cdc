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
import org.apache.flink.cdc.connectors.elasticsearch.serializer.Elasticsearch6RequestCreator;
import org.apache.flink.cdc.connectors.elasticsearch.serializer.ElasticsearchEventSerializer;
import org.apache.flink.cdc.connectors.elasticsearch.v2.Elasticsearch8AsyncSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * An implementation of {@link DataSink} for writing events to Elasticsearch.
 *
 * <p>This class is responsible for configuring and managing the lifecycle of an Elasticsearch sink,
 * including handling different versions of Elasticsearch (6, 7, 8).
 *
 * @param <InputT> The type of input elements that this sink can process.
 */
public class ElasticsearchDataSink<InputT> implements DataSink, Serializable {

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
        switch (elasticsearchOptions.getVersion()) {
            case 6:
                return getElasticsearch6SinkProvider();
            case 7:
                return getElasticsearch7SinkProvider();
            case 8:
                return getElasticsearch8SinkProvider();
            default:
                throw new IllegalArgumentException(
                        "Unsupported Elasticsearch version: " + elasticsearchOptions.getVersion());
        }
    }

    private EventSinkProvider getElasticsearch6SinkProvider() {
        ElasticsearchEventSerializer serializer = new ElasticsearchEventSerializer(zoneId);
        org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost[] hosts =
                elasticsearchOptions.getHosts().stream()
                        .map(
                                host ->
                                        new org.apache.flink.elasticsearch6.shaded.org.apache.http
                                                .HttpHost(
                                                host.getHostName(),
                                                host.getPort(),
                                                host.getSchemeName()))
                        .toArray(
                                org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost[]
                                        ::new);

        return FlinkSinkProvider.of(
                new Elasticsearch6SinkBuilder<Event>()
                        .setHosts(hosts)
                        .setEmitter(
                                (element, context, indexer) -> {
                                    BulkOperationVariant operation =
                                            serializer.apply(element, context);
                                    if (operation instanceof IndexOperation) {
                                        indexer.add(
                                                Elasticsearch6RequestCreator.createIndexRequest(
                                                        (IndexOperation<?>) operation));
                                    } else if (operation instanceof DeleteOperation) {
                                        indexer.add(
                                                Elasticsearch6RequestCreator.createDeleteRequest(
                                                        (DeleteOperation) operation));
                                    }
                                })
                        .setBulkFlushMaxActions(elasticsearchOptions.getMaxBatchSize())
                        .setBulkFlushInterval(elasticsearchOptions.getMaxTimeInBufferMS())
                        .build());
    }

    private EventSinkProvider getElasticsearch7SinkProvider() {
        ElasticsearchEventSerializer serializer = new ElasticsearchEventSerializer(zoneId);
        org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost[] hosts =
                elasticsearchOptions.getHosts().stream()
                        .map(
                                host ->
                                        new org.apache.flink.elasticsearch6.shaded.org.apache.http
                                                .HttpHost(
                                                host.getHostName(),
                                                host.getPort(),
                                                host.getSchemeName()))
                        .toArray(
                                org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost[]
                                        ::new);

        return FlinkSinkProvider.of(
                new Elasticsearch6SinkBuilder<Event>()
                        .setHosts(hosts)
                        .setEmitter(
                                (element, context, indexer) -> {
                                    BulkOperationVariant operation =
                                            serializer.apply(element, context);
                                    if (operation instanceof IndexOperation) {
                                        indexer.add(
                                                Elasticsearch6RequestCreator.createIndexRequest(
                                                        (IndexOperation<?>) operation));
                                    } else if (operation instanceof DeleteOperation) {
                                        indexer.add(
                                                Elasticsearch6RequestCreator.createDeleteRequest(
                                                        (DeleteOperation) operation));
                                    }
                                })
                        .setBulkFlushMaxActions(elasticsearchOptions.getMaxBatchSize())
                        .setBulkFlushInterval(elasticsearchOptions.getMaxTimeInBufferMS())
                        .build());
    }

    private EventSinkProvider getElasticsearch8SinkProvider() {
        return FlinkSinkProvider.of(
                new Elasticsearch8AsyncSinkBuilder<Event>()
                        .setHosts(
                                elasticsearchOptions
                                        .getHosts()
                                        .toArray(new org.apache.http.HttpHost[0]))
                        .setElementConverter(new ElasticsearchEventSerializer(zoneId))
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
