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

package com.ververica.cdc.connectors.kafka.sink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaDataSinkFactory}. */
public class KafkaDataSinkFactoryTest {

    @Test
    public void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSinkFactory.class);
        Assertions.assertTrue(sinkFactory instanceof KafkaDataSinkFactory);

        Configuration conf = Configuration.fromMap(ImmutableMap.<String, String>builder().build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertTrue(dataSink instanceof KafkaDataSink);
    }
}
