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

package com.ververica.cdc.composer.flink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import com.ververica.cdc.composer.utils.factory.DataSinkFactory1;
import org.junit.Assert;
import org.junit.Test;

/** A test for the {@link FlinkPipelineComposer}. */
public class FlinkPipelineComposerTest {

    @Test
    public void testCreateDataSinkFromSinkDef() {
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

        Assert.assertTrue(dataSink instanceof DataSinkFactory1.TestDataSink);
        Assert.assertEquals("0.0.0.0", ((DataSinkFactory1.TestDataSink) dataSink).getHost());
    }
}
