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

package org.apache.flink.cdc.connectors.doris.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DorisDataSinkFactory}. */
class DorisDataSinkFactoryTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCompressionDisabledByDefault(boolean batchMode) {
        assertCompression(createConfiguration(batchMode), "");
    }

    @ParameterizedTest
    @CsvSource({"true, ''", "false, ''", "true, gz", "false, gz"})
    void testExplicitCompression(boolean batchMode, String compressType) {
        Map<String, String> config = createConfiguration(batchMode);
        config.put("sink.properties.compress_type", compressType);
        assertCompression(config, compressType);
    }

    private Map<String, String> createConfiguration(boolean batchMode) {
        Map<String, String> config = new HashMap<>();
        config.put("fenodes", "localhost:8030");
        config.put("username", "root");
        config.put("sink.enable.batch-mode", Boolean.toString(batchMode));
        config.put("sink.properties.strict_mode", "true");
        return config;
    }

    private void assertCompression(Map<String, String> config, String expectedCompressType) {
        DataSink sink =
                new DorisDataSinkFactory()
                        .createDataSink(
                                new FactoryHelper.DefaultContext(
                                        Configuration.fromMap(config),
                                        Configuration.fromMap(
                                                Collections.singletonMap("local-time-zone", "UTC")),
                                        getClass().getClassLoader()));

        assertThat(sink)
                .extracting("executionOptions")
                .satisfies(
                        value -> {
                            DorisExecutionOptions options = (DorisExecutionOptions) value;
                            assertThat(options.getStreamLoadProp())
                                    .containsEntry("compress_type", expectedCompressType)
                                    .containsEntry("format", "json")
                                    .containsEntry("read_json_by_line", true)
                                    .containsEntry("strict_mode", "true");
                            assertThat(options.isGzipCompressionEnabled())
                                    .isEqualTo("gz".equals(expectedCompressType));
                            assertThat(options.enableBatchMode())
                                    .isEqualTo(
                                            Boolean.parseBoolean(
                                                    config.get("sink.enable.batch-mode")));
                        });
    }
}
