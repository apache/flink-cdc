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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.composer.utils.factory.DataSinkFactory1;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSource;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.configuration.DeploymentOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testGettingFlinkConfiguration() {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        PipelineDef pipelineDef =
                new PipelineDef(
                        new SourceDef(TestDataSourceFactory.IDENTIFIER, null, new Configuration()),
                        new SinkDef(TestDataSinkFactory.IDENTIFIER, null, new Configuration()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new Configuration());
        // No exception will be thrown.
        composer.compose(pipelineDef);
    }

    /** A dummy {@link DataSinkFactory} that validates the execution target. */
    public static class TestDataSinkFactory implements DataSinkFactory {

        public static final String IDENTIFIER = "test-sink-factory";

        @Override
        public DataSink createDataSink(Context context) {
            // This option has no default value.
            String target = context.getFlinkConf().get(DeploymentOptions.TARGET);
            if (!"local".equals(target)) {
                throw new IllegalArgumentException(
                        "The flink configuration is invalid. Please check the pipeline configuration.");
            }
            return new DataSink() {
                @Override
                public EventSinkProvider getEventSinkProvider() {
                    return null;
                }

                @Override
                public MetadataApplier getMetadataApplier() {
                    return schemaChangeEvent -> {};
                }
            };
        }

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return new HashSet<>();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return new HashSet<>();
        }
    }

    /** A dummy {@link DataSourceFactory} that validates the execution target. */
    public static class TestDataSourceFactory implements DataSourceFactory {

        public static final String IDENTIFIER = "test-source-factory";

        @Override
        public DataSource createDataSource(Context context) {
            // This option has no default value.
            String target = context.getFlinkConf().get(DeploymentOptions.TARGET);
            if (!"local".equals(target)) {
                throw new IllegalArgumentException(
                        "The flink configuration is invalid. Please check the pipeline configuration.");
            }
            return new ValuesDataSource(
                    ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE, Integer.MAX_VALUE);
        }

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return new HashSet<>();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return new HashSet<>();
        }
    }

    @ParameterizedTest
    @MethodSource
    void testInvalidPipelineConfiguration(Configuration pipelineConfig) {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        PipelineDef pipelineDef = buildPipelineDefinitionFromConfiguration(pipelineConfig);

        assertThatThrownBy(() -> composer.compose(pipelineDef))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> testInvalidPipelineConfiguration() {
        Configuration configuration1 = new Configuration();
        configuration1.set(PipelineOptions.PIPELINE_OPERATOR_UID_PREFIX, "junit");
        configuration1.set(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID, "schema-operator");

        return Stream.of(Arguments.of(configuration1));
    }

    @ParameterizedTest
    @MethodSource
    void testValidPipelineConfiguration(Configuration pipelineConfig) {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        PipelineDef pipelineDef = buildPipelineDefinitionFromConfiguration(pipelineConfig);
        composer.compose(pipelineDef);
    }

    static Stream<Arguments> testValidPipelineConfiguration() {
        Configuration configuration1 = new Configuration();

        Configuration configuration2 = new Configuration();
        configuration2.set(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID, "schema-operator");

        Configuration configuration3 = new Configuration();
        configuration3.set(PipelineOptions.PIPELINE_OPERATOR_UID_PREFIX, "junit");

        return Stream.of(
                Arguments.of(configuration1),
                Arguments.of(configuration2),
                Arguments.of(configuration3));
    }

    PipelineDef buildPipelineDefinitionFromConfiguration(Configuration pipelineConfig) {
        return new PipelineDef(
                new SourceDef(ValuesDataFactory.IDENTIFIER, null, new Configuration()),
                new SinkDef(ValuesDataFactory.IDENTIFIER, null, new Configuration()),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                pipelineConfig);
    }
}
