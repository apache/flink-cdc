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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.List;

/** Provider of {@link SchemaRegistry}. */
@Internal
public class SchemaRegistryProvider implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;

    private final OperatorID operatorID;
    private final String operatorName;
    private final MetadataApplier metadataApplier;
    private final List<RouteRule> routingRules;

    public SchemaRegistryProvider(
            OperatorID operatorID,
            String operatorName,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules) {
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        return new SchemaRegistry(operatorName, context, metadataApplier, routingRules);
    }
}
