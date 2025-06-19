package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlussDataSinkFactoryTest {
    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf = createValidConfiguration();
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(FlussDataSink.class);
    }

    @Test
    void testUnsupportedOption() {

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                                .put(FlussDataSinkOptions.DATABASE.key(), "mydb")
                                .put(FlussDataSinkOptions.TABLE.key(), "orders")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'fluss'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                                .put(FlussDataSinkOptions.DATABASE.key(), "mydb")
                                .put(FlussDataSinkOptions.TABLE.key(), "orders")
                                .put("fluss.properties.option1", "value1")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(FlussDataSink.class);
    }

    private Configuration createValidConfiguration() {
        return Configuration.fromMap(
                ImmutableMap.<String, String>builder()
                        .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                        .put(FlussDataSinkOptions.DATABASE.key(), "mydb")
                        .put(FlussDataSinkOptions.TABLE.key(), "orders")
                        .build());
    }
}
