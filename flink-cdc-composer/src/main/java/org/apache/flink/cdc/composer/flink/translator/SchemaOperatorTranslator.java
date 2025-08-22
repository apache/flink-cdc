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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.runtime.operators.schema.regular.BatchSchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperatorFactory;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Translator used to build {@link SchemaOperator} for schema event process. */
@Internal
public class SchemaOperatorTranslator {
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final String schemaOperatorUid;
    private final Duration rpcTimeOut;
    private final String timezone;

    public SchemaOperatorTranslator(
            SchemaChangeBehavior schemaChangeBehavior,
            String schemaOperatorUid,
            Duration rpcTimeOut,
            String timezone) {
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaOperatorUid = schemaOperatorUid;
        this.rpcTimeOut = rpcTimeOut;
        this.timezone = timezone;
    }

    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return translateRegular(input, parallelism, false, metadataApplier, routes);
    }

    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int parallelism,
            boolean isBatchMode,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {

        return isBatchMode
                ? addRegularSchemaBatchOperator(
                        input, parallelism, metadataApplier, routes, timezone)
                : addRegularSchemaOperator(
                        input,
                        parallelism,
                        metadataApplier,
                        routes,
                        schemaChangeBehavior,
                        timezone);
    }

    public DataStream<Event> translateDistributed(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return addDistributedSchemaOperator(
                input, parallelism, metadataApplier, routes, schemaChangeBehavior, timezone);
    }

    @Deprecated
    public String getSchemaOperatorUid() {
        return schemaOperatorUid;
    }

    private DataStream<Event> addRegularSchemaOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaOperator",
                        new EventTypeInfo(),
                        new SchemaOperatorFactory(
                                metadataApplier,
                                routingRules,
                                rpcTimeOut,
                                schemaChangeBehavior,
                                timezone));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }

    private DataStream<Event> addRegularSchemaBatchOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            String timezone) {
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaBatchOperator",
                        new EventTypeInfo(),
                        new BatchSchemaOperator(routingRules, metadataApplier, timezone));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }

    private DataStream<Event> addDistributedSchemaOperator(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        Preconditions.checkArgument(
                schemaChangeBehavior == SchemaChangeBehavior.LENIENT
                        || schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                        || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION,
                "Schema change behavior %s is not supported because you're trying to compose a "
                        + "pipeline with distributed topology, where data records from different partitions needs to "
                        + "be combined together.\n"
                        + "Use `LENIENT` mode to evolve downstream schema while keep all upstream data fields intact.\n"
                        + "Use `IGNORE` to get a static schema view and ignore any upstream schema changes.\n"
                        + "Use `EXCEPTION` to report error immediately as upstream schema changes are unacceptable.",
                schemaChangeBehavior);
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        return input.transform(
                        "SchemaMapper",
                        new EventTypeInfo(),
                        new org.apache.flink.cdc.runtime.operators.schema.distributed
                                .SchemaOperatorFactory(
                                metadataApplier,
                                routingRules,
                                rpcTimeOut,
                                schemaChangeBehavior,
                                timezone))
                .uid(schemaOperatorUid)
                .setParallelism(parallelism);
    }
}
