/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

/** A mock context for serialization schema testing. */
public class MockInitializationContext implements SerializationSchema.InitializationContext {

    @Override
    public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(MockInitializationContext.class.getClassLoader());
    }
}
