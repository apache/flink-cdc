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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.composer.utils.factory.DataSinkFactory1;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** A test for the {@link FlinkPipelineComposer}. */
class FlinkPipelineComposerTest {

    @Test
    void testCreateDataSinkFromSinkDef() {
        SinkDef sinkDef =
                new SinkDef(
                        "data-sink-factory-1",
                        "sink-database",
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("host", "0.0.0.0")
                                        .build()));

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                sinkDef.getConfig(),
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSink).isExactlyInstanceOf(DataSinkFactory1.TestDataSink.class);
        Assertions.assertThat(((DataSinkFactory1.TestDataSink) dataSink).getHost())
                .isEqualTo("0.0.0.0");
    }
}
