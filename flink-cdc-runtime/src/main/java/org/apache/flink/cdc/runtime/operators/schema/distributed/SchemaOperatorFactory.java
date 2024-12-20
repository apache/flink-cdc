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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;

import java.time.Duration;
import java.util.List;

/** Factory to create {@link SchemaOperator}. */
@Internal
public class SchemaOperatorFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>,
                OneInputStreamOperatorFactory<PartitioningEvent, Event> {
    private static final long serialVersionUID = 1L;

    private final MetadataApplier metadataApplier;
    private final List<RouteRule> routingRules;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final Duration rpcTimeout;

    public SchemaOperatorFactory(
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        super(new SchemaOperator(routingRules, rpcTimeout, schemaChangeBehavior, timezone));
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.rpcTimeout = rpcTimeout;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SchemaCoordinatorProvider(
                operatorID,
                operatorName,
                metadataApplier,
                routingRules,
                schemaChangeBehavior,
                rpcTimeout);
    }
}
