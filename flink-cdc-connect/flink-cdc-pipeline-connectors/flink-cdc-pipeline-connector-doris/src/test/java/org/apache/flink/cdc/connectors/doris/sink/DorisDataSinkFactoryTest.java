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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.doris.factory.DorisDataSinkFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link DorisDataSinkFactory}. */
class DorisDataSinkFactoryTest {

    @Test
    void testCreateDataSinkWithDefaultJsonStreamLoadProperties() {
        DataSink dataSink = createDataSink(Configuration.fromMap(createValidOptions()));

        Assertions.assertThat(dataSink).isInstanceOf(DorisDataSink.class);
    }

    @Test
    void testCreateEventSinkProviderWithCsvStreamLoadFormat() {
        Map<String, String> options = createValidOptions();
        options.put("sink.properties.format", "csv");
        DorisDataSink dataSink = (DorisDataSink) createDataSink(Configuration.fromMap(options));

        Assertions.assertThatCode(dataSink::getEventSinkProvider).doesNotThrowAnyException();
    }

    @Test
    void testCreateDataSinkAcceptsSchemaChangeOptions() {
        Map<String, String> options = createValidOptions();
        options.put("schema.change.null_enable", "false");
        options.put("schema.change.default_value", "false");

        Assertions.assertThatCode(() -> createDataSink(Configuration.fromMap(options)))
                .doesNotThrowAnyException();
    }

    private DataSink createDataSink(Configuration conf) {
        DorisDataSinkFactory sinkFactory = new DorisDataSinkFactory();
        Configuration pipelineConf = new Configuration();
        pipelineConf.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "UTC");
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        conf, pipelineConf, Thread.currentThread().getContextClassLoader()));
    }

    private Map<String, String> createValidOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("fenodes", "127.0.0.1:8030");
        options.put("username", "root");
        return options;
    }
}
