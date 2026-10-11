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

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSink;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions;
import org.apache.flink.cdc.connectors.dws.sink.v2.DwsSink;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

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

        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        Assertions.assertThat(eventSinkProvider).isInstanceOf(FlinkSinkProvider.class);
        Assertions.assertThat(((FlinkSinkProvider) eventSinkProvider).getSink())
                .isInstanceOf(DwsSink.class)
                .isNotInstanceOf(TwoPhaseCommittingSink.class);
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
    void testDatabaseOptionIsRejectedBecauseJdbcUrlSelectsDatabase() {
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
                                .put("database", "ignored_database")
                                .build());

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'dws'")
                .hasMessageContaining("database");
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
        factoryConfiguration.set(
                DwsDataSinkOptions.DWS_CLIENT_RETRY_SLEEP_BASE_TIME, Duration.ofSeconds(2));
        factoryConfiguration.set(
                DwsDataSinkOptions.DWS_CLIENT_RETRY_SLEEP_RANDOM_TIME, Duration.ofMillis(125));
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_TIMEOUT_TASK, Duration.ofMinutes(4));
        factoryConfiguration.set(
                DwsDataSinkOptions.DWS_CLIENT_TIMEOUT_STATEMENT, Duration.ofMinutes(2));
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_BUFFER_ALL_MAX_BYTES, "12MiB");
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_BUFFER_TABLE_MAX_BYTES, "8MiB");
        factoryConfiguration.set(DwsDataSinkOptions.DWS_CLIENT_BUFFER_PARTITION_MAX_BYTES, "4MiB");

        Configuration pipelineConfiguration = new Configuration();
        pipelineConfiguration.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "Asia/Shanghai");

        DwsDataSink dataSink =
                (DwsDataSink)
                        createDataSink(sinkFactory, factoryConfiguration, pipelineConfiguration);
        MetadataApplier metadataApplier = dataSink.getMetadataApplier();

        DwsDataSinkConfig sinkConfig = (DwsDataSinkConfig) readField(dataSink, "sinkConfig");
        Assertions.assertThat(dataSink.requiresPrimaryKeyUpdateSplit()).isTrue();
        Assertions.assertThat(sinkConfig.getZoneId()).isEqualTo(ZoneId.of("UTC"));
        Assertions.assertThat(sinkConfig.getDefaultSchema()).isEqualTo("ods");
        Assertions.assertThat(sinkConfig.isEnableDelete()).isEqualTo(false);
        Assertions.assertThat(sinkConfig.getRetrySleepBaseTime()).isEqualTo(Duration.ofSeconds(2));
        Assertions.assertThat(sinkConfig.getRetrySleepRandomTime())
                .isEqualTo(Duration.ofMillis(125));
        Assertions.assertThat(sinkConfig.getTaskTimeout()).isEqualTo(Duration.ofMinutes(4));
        Assertions.assertThat(sinkConfig.getStatementTimeout()).isEqualTo(Duration.ofMinutes(2));
        Assertions.assertThat(sinkConfig.getBufferAllMaxBytes()).isEqualTo(12L * 1024 * 1024);
        Assertions.assertThat(sinkConfig.getBufferTableMaxBytes()).isEqualTo(8L * 1024 * 1024);
        Assertions.assertThat(sinkConfig.getBufferPartitionMaxBytes()).isEqualTo(4L * 1024 * 1024);
        Assertions.assertThat(readField(metadataApplier, "defaultSchema")).isEqualTo("ods");
        Assertions.assertThat(readField(metadataApplier, "enableDnPartition")).isEqualTo(true);
        Assertions.assertThat(readField(metadataApplier, "distributionKey")).isEqualTo("id");

        Assertions.assertThat(sinkConfig.getUrl())
                .isEqualTo(
                        factoryConfiguration.get(DwsDataSinkOptions.URL)
                                + "?connectTimeout=10&socketTimeout=60");
        Assertions.assertThat(sinkConfig.getUsername())
                .isEqualTo(factoryConfiguration.get(DwsDataSinkOptions.USERNAME));
        Assertions.assertThat(sinkConfig.getPassword())
                .isEqualTo(factoryConfiguration.get(DwsDataSinkOptions.PASSWORD));
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

    @Test
    void testDistributionOptionsMustBeConfiguredTogether() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Configuration missingKey = createRequiredConfiguration();
        missingKey.set(DwsDataSinkOptions.ENABLE_DN_PARTITION, true);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, missingKey, missingKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distribution-key is required");

        Configuration disabled = createRequiredConfiguration();
        disabled.set(DwsDataSinkOptions.DISTRIBUTION_KEY, "id");
        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, disabled, disabled))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires enable-dn-partition=true");
    }

    @Test
    void testRejectsInvalidBufferHierarchyAndNonPositiveTimeouts() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Configuration invalidHierarchy = createRequiredConfiguration();
        invalidHierarchy.set(DwsDataSinkOptions.DWS_CLIENT_BUFFER_ALL_MAX_BYTES, "4MiB");
        invalidHierarchy.set(DwsDataSinkOptions.DWS_CLIENT_BUFFER_TABLE_MAX_BYTES, "8MiB");

        Assertions.assertThatThrownBy(
                        () -> createDataSink(sinkFactory, invalidHierarchy, invalidHierarchy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition <= table <= all");

        Configuration invalidTimeout = createRequiredConfiguration();
        invalidTimeout.set(DwsDataSinkOptions.DWS_CLIENT_TIMEOUT_TASK, Duration.ZERO);
        Assertions.assertThatThrownBy(
                        () -> createDataSink(sinkFactory, invalidTimeout, invalidTimeout))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("task timeout must be positive");
    }

    @Test
    void testAcceptsEquivalentAliasesAndRejectsConflicts() throws Exception {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Configuration equivalent =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .putAll(createRequiredConfiguration().toMap())
                                .put("write-mode", "AUTO")
                                .put("dws.client.write.mode", "auto")
                                .put("auto-flush-max-interval", "3 s")
                                .put("dws.client.write.auto-flush-max-interval", "3000 ms")
                                .put("connectionMaxUseTimeSeconds", "60")
                                .put("dws.client.jdbc.max.use-time", "1 min")
                                .build());

        DwsDataSink dataSink = (DwsDataSink) createDataSink(sinkFactory, equivalent, equivalent);
        DwsDataSinkConfig sinkConfig = (DwsDataSinkConfig) readField(dataSink, "sinkConfig");
        Assertions.assertThat(sinkConfig.getConnectionMaxUseTime())
                .isEqualTo(Duration.ofMinutes(1));

        Configuration conflicting = new Configuration(equivalent);
        conflicting.set(DwsDataSinkOptions.WRITE_MODE, "upsert");
        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conflicting, conflicting))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Conflicting values")
                .hasMessageContaining("dws.client.write.mode");
    }

    @Test
    void testMergesUrlTimeoutsWithoutOverwritingUnrelatedParameters() throws Exception {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Configuration conf = createRequiredConfiguration();
        conf.set(
                DwsDataSinkOptions.URL,
                "jdbc:gaussdb://localhost:8000/test?currentSchema=ods&connectTimeout=7");
        conf.set(DwsDataSinkOptions.CONNECTION_TIME_OUT, 7000L);
        conf.set(DwsDataSinkOptions.CONNECTION_SOCKET_TIMEOUT, 12_000);

        DwsDataSink dataSink = (DwsDataSink) createDataSink(sinkFactory, conf, conf);
        DwsDataSinkConfig sinkConfig = (DwsDataSinkConfig) readField(dataSink, "sinkConfig");
        Assertions.assertThat(sinkConfig.getUrl())
                .isEqualTo(
                        "jdbc:gaussdb://localhost:8000/test?currentSchema=ods&connectTimeout=7&socketTimeout=12");

        Configuration conflict = new Configuration(conf);
        conflict.set(DwsDataSinkOptions.CONNECTION_TIME_OUT, 8000L);
        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conflict, conflict))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Conflicting timeout values");
    }

    @Test
    void testRejectsNativePartitionOverridesWithScalingGuidance() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Configuration conf = createRequiredConfiguration();
        conf.set(DwsDataSinkOptions.DWS_CLIENT_WRITE_PARTITION_MAX, 2);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf, conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition-max")
                .hasMessageContaining("pipeline.parallelism");
    }

    @Test
    void testRejectsEveryDocumentedDisabledOptionInsteadOfIgnoringIt() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("dws", DataSinkFactory.class);
        Map<String, String> disabled = new LinkedHashMap<>();
        disabled.put("sink-table", "orders");
        disabled.put("sink.parallelism", "2");
        disabled.put("connectionSize", "2");
        disabled.put("connectionPoolName", "pool");
        disabled.put("connectionPoolSize", "2");
        disabled.put("connectionPoolTimeout", "1000");
        disabled.put("connectionMaxUseCount", "100");
        disabled.put("needConnectionPoolMonitor", "true");
        disabled.put("connectionPoolMonitorPeriod", "1000");
        disabled.put("logSwitch", "true");
        disabled.put("dws.client.write.partition-policy", "hash");
        disabled.put("dws.client.write.partition-min", "2");
        disabled.put("dws.client.write.partition-max", "2");

        disabled.forEach(
                (key, value) -> {
                    Configuration conf =
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .putAll(createRequiredConfiguration().toMap())
                                            .put(key, value)
                                            .build());
                    Assertions.assertThatThrownBy(
                                    () -> createDataSink(sinkFactory, conf, conf), key)
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining(key);
                });
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
