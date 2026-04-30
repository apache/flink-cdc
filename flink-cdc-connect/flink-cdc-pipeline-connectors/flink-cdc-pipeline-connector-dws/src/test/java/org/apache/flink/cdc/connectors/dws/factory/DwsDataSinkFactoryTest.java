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

package org.apache.flink.cdc.connectors.dws.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSink;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.huaweicloud.dws.client.config.DwsClientConfigs;
import com.huaweicloud.dws.client.model.PartitionPolicy;
import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;

/** Tests for {@link DwsDataSinkFactory}. */
class DwsDataSinkFactoryTest {

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(DwsDataSinkFactory.class);

        DataSink dataSink =
                createDataSink(sinkFactory, createRequiredConfiguration(), new Configuration());
        Assertions.assertThat(dataSink).isInstanceOf(DwsDataSink.class);
    }

    @Test
    void testUnsupportedOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(DwsDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        DwsDataSinkOptions.URL.key(),
                                        "jdbc:gaussdb://localhost:8000/test")
                                .put(DwsDataSinkOptions.USERNAME.key(), "user")
                                .put(DwsDataSinkOptions.PASSWORD.key(), "password")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'dws'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testCreateDataSinkWithConfiguredOptions() throws Exception {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(DwsDataSinkFactory.class);

        Configuration factoryConfiguration = createRequiredConfiguration();
        factoryConfiguration.set(DwsDataSinkOptions.PIPELINE_LOCAL_TIME_ZONE, "UTC");
        factoryConfiguration.set(DwsDataSinkOptions.SCHEMA, "ods");
        factoryConfiguration.set(DwsDataSinkOptions.ENABLE_AUTO_FLUSH, false);
        factoryConfiguration.set(DwsDataSinkOptions.ENABLE_DN_PARTITION, true);
        factoryConfiguration.set(DwsDataSinkOptions.DISTRIBUTION_KEY, "id");
        factoryConfiguration.set(DwsDataSinkOptions.SINK_ENABLE_DELETE, false);
        factoryConfiguration.set(DwsDataSinkOptions.SINK_MAX_RETRIES, 9);
        factoryConfiguration.set(DwsDataSinkOptions.WRITE_MODE, "auto");
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_WRITE_THREAD_SIZE, 8);
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_WRITE_USE_COPY_SIZE, 5000);
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_WRITE_FORCE_FLUSH_SIZE, 6000);

        Configuration pipelineConfiguration = new Configuration();
        pipelineConfiguration.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "Asia/Shanghai");

        DwsDataSink dataSink =
                (DwsDataSink)
                        createDataSink(sinkFactory, factoryConfiguration, pipelineConfiguration);
        MetadataApplier metadataApplier = dataSink.getMetadataApplier();

        Assertions.assertThat(readField(dataSink, "zoneId")).isEqualTo(ZoneId.of("UTC"));
        Assertions.assertThat(readField(dataSink, "defaultSchema")).isEqualTo("ods");
        Assertions.assertThat(readField(dataSink, "enableAutoFlush")).isEqualTo(false);
        Assertions.assertThat(readField(dataSink, "enableDelete")).isEqualTo(false);
        Assertions.assertThat(readField(dataSink, "writeMode")).isEqualTo(WriteMode.AUTO);
        Assertions.assertThat(readField(metadataApplier, "defaultSchema")).isEqualTo("ods");
        Assertions.assertThat(readField(metadataApplier, "enableDnPartition")).isEqualTo(true);
        Assertions.assertThat(readField(metadataApplier, "distributionKey")).isEqualTo("id");

        DwsConnectionOptions connectorOptions =
                (DwsConnectionOptions) readField(dataSink, "connectorOptions");
        Assertions.assertThat(
                        connectorOptions.getConfig().get(DwsClientConfigs.WRITE_PARTITION_POLICY))
                .isEqualTo(PartitionPolicy.DN);
        Assertions.assertThat(connectorOptions.getConfig().get(DwsClientConfigs.WRITE_THREAD_SIZE))
                .isEqualTo(8);
        Assertions.assertThat(
                        connectorOptions
                                .getConfig()
                                .get(DwsClientConfigs.WRITE_USE_COPY_BATCH_SIZE))
                .isEqualTo(5000);
        Assertions.assertThat(
                        connectorOptions
                                .getConfig()
                                .get(DwsClientConfigs.WRITE_FORCE_FLUSH_BATCH_SIZE))
                .isEqualTo(6000);
        Assertions.assertThat(connectorOptions.getConfig().get(DwsClientConfigs.RETRY_MAX_TIMES))
                .isEqualTo(9);
    }

    @Test
    void testInvalidWriteMode() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(DwsDataSinkFactory.class);

        Configuration conf = createRequiredConfiguration();
        conf.set(DwsDataSinkOptions.WRITE_MODE, "not-supported");

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf, conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported write-mode 'not-supported'");
    }

    private static DataSink createDataSink(
            DataSinkFactory sinkFactory,
            Configuration factoryConfiguration,
            Configuration pipelineConfiguration) {
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        factoryConfiguration,
                        pipelineConfiguration,
                        Thread.currentThread().getContextClassLoader()));
    }

    private static Configuration createRequiredConfiguration() {
        return Configuration.fromMap(
                ImmutableMap.<String, String>builder()
                        .put(DwsDataSinkOptions.URL.key(), "jdbc:gaussdb://localhost:8000/test")
                        .put(DwsDataSinkOptions.USERNAME.key(), "user")
                        .put(DwsDataSinkOptions.PASSWORD.key(), "password")
                        .build());
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
