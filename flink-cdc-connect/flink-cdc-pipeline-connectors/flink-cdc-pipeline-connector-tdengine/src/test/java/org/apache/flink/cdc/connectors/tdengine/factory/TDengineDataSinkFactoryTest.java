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

package org.apache.flink.cdc.connectors.tdengine.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSink;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link TDengineDataSinkFactory}. */
class TDengineDataSinkFactoryTest {

    @Test
    void testCreateDataSink() throws Exception {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("tdengine", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(TDengineDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("url", "jdbc:TAOS-WS://localhost:6041")
                                .put("username", "root")
                                .put("password", "taosdata")
                                .put("database-name", "metrics_db")
                                .put("stable.name", "device_metrics")
                                .put("timestamp.field", "ts")
                                .put("subtable.field", "device_id")
                                .put("tag.fields", "location,group_id")
                                .put("sink.flush.max-rows", "10")
                                .put("sink.max-sql-bytes", "1024")
                                .put("sink.flush.interval", "5s")
                                .put("sink.max-retries", "2")
                                .put("sink.retry.backoff", "100ms")
                                .put("sink.delete.mode", "ignore")
                                .put("sink.update.mode", "upsert")
                                .put("sink.timestamp-change.mode", "allow")
                                .put("sink.subtable-change.mode", "allow")
                                .put("connection.properties.charset", "UTF-8")
                                .build());

        DataSink dataSink = createDataSink(sinkFactory, conf);

        Assertions.assertThat(dataSink).isInstanceOf(TDengineDataSink.class);
        TDengineDataSinkConfig config = extractConfig(dataSink);
        Assertions.assertThat(config.getUrl()).isEqualTo("jdbc:TAOS-WS://localhost:6041");
        Assertions.assertThat(config.getDatabaseName()).isEqualTo("metrics_db");
        Assertions.assertThat(config.getStableName()).isEqualTo("device_metrics");
        Assertions.assertThat(config.getTagFields()).containsExactly("location", "group_id");
        Assertions.assertThat(config.getFlushMaxRows()).isEqualTo(10);
        Assertions.assertThat(config.getMaxSqlBytes()).isEqualTo(1024);
        Assertions.assertThat(config.getDeleteMode()).isEqualTo("ignore");
        Assertions.assertThat(config.getUpdateMode()).isEqualTo("upsert");
        Assertions.assertThat(config.getConnectionProperties()).containsEntry("charset", "UTF-8");
    }

    @Test
    void testRejectNonWebSocketUrl() {
        DataSinkFactory sinkFactory = new TDengineDataSinkFactory();
        Map<String, String> options = validOptions();
        options.put("url", "jdbc:TAOS-RS://localhost:6041");
        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("jdbc:TAOS-WS://");
    }

    @Test
    void testRejectMissingTagFields() {
        DataSinkFactory sinkFactory = new TDengineDataSinkFactory();
        Map<String, String> options = validOptions();
        options.put("tag.fields", "");
        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("tag.fields");
    }

    @Test
    void testRejectOverlappedFieldRoles() {
        DataSinkFactory sinkFactory = new TDengineDataSinkFactory();
        Map<String, String> options = validOptions();
        options.put("tag.fields", "device_id");
        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("subtable.field");
    }

    @Test
    void testRejectNonPositiveMaxSqlBytes() {
        DataSinkFactory sinkFactory = new TDengineDataSinkFactory();
        Map<String, String> options = validOptions();
        options.put("sink.max-sql-bytes", "0");
        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.max-sql-bytes");
    }

    @Test
    void testRejectAppendUpdateMode() {
        DataSinkFactory sinkFactory = new TDengineDataSinkFactory();
        Map<String, String> options = validOptions();
        options.put("sink.update.mode", "append");
        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.update.mode");
    }

    private static ImmutableMap.Builder<String, String> validConfigBuilder() {
        return ImmutableMap.<String, String>builder()
                .put("url", "jdbc:TAOS-WS://localhost:6041")
                .put("database-name", "metrics_db")
                .put("timestamp.field", "ts")
                .put("subtable.field", "device_id")
                .put("tag.fields", "location");
    }

    private static Map<String, String> validOptions() {
        return new HashMap<>(validConfigBuilder().build());
    }

    private static DataSink createDataSink(DataSinkFactory sinkFactory, Configuration conf) {
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        conf, conf, Thread.currentThread().getContextClassLoader()));
    }

    private static TDengineDataSinkConfig extractConfig(DataSink dataSink) throws Exception {
        Field field = TDengineDataSink.class.getDeclaredField("config");
        field.setAccessible(true);
        return (TDengineDataSinkConfig) field.get(dataSink);
    }
}
