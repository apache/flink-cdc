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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GaussDBSourceTest {

    @Test
    void builderCreatesDebeziumSourceFunction() throws Exception {
        DebeziumSourceFunction<String> sourceFunction =
                GaussDBSource.<String>builder()
                        .hostname("localhost")
                        .database("customers")
                        .username("gaussdb")
                        .password("pwd")
                        .schemaList("public")
                        .tableList("public.orders")
                        .deserializer(new TestingDeserializationSchema<>())
                        .build();

        Properties properties = extractProperties(sourceFunction);

        assertThat(sourceFunction).isNotNull();
        assertThat(properties.getProperty("connector.class"))
                .isEqualTo("io.debezium.connector.gaussdb.GaussDBConnector");
        assertThat(properties.getProperty("plugin.name")).isEqualTo("mppdb_decoding");
        assertThat(properties.getProperty("database.hostname")).isEqualTo("localhost");
        assertThat(properties.getProperty("database.port")).isEqualTo("8000");
        assertThat(properties.getProperty("database.dbname")).isEqualTo("customers");
        assertThat(properties.getProperty("database.user")).isEqualTo("gaussdb");
        assertThat(properties.getProperty("database.password")).isEqualTo("pwd");
        assertThat(properties.getProperty("schema.include.list")).isEqualTo("public");
        assertThat(properties.getProperty("table.include.list")).isEqualTo("public.orders");
        assertThat(properties.getProperty("ha-port")).isNull();
        assertThat(properties.getProperty("heartbeat.interval.ms"))
                .isEqualTo(String.valueOf(Duration.ofMinutes(5).toMillis()));
    }

    @Test
    void appliesGaussDBSpecificParameters() throws Exception {
        Properties customProperties = new Properties();
        customProperties.setProperty("snapshot.mode", "never");

        DebeziumSourceFunction<String> sourceFunction =
                GaussDBSource.<String>builder()
                        .hostname("db.internal")
                        .port(19000)
                        .haPort(20010)
                        .database("inventory")
                        .username("tester")
                        .password("secret")
                        .schemaList("analytics")
                        .tableList("analytics.events")
                        .slotName("gauss_slot")
                        .decodingPluginName("test_plugin")
                        .debeziumProperties(customProperties)
                        .deserializer(new TestingDeserializationSchema<>())
                        .build();

        Properties properties = extractProperties(sourceFunction);

        assertThat(properties.getProperty("plugin.name")).isEqualTo("test_plugin");
        assertThat(properties.getProperty("slot.name")).isEqualTo("gauss_slot");
        assertThat(properties.getProperty("ha-port")).isEqualTo("20010");
        assertThat(properties.getProperty("database.port")).isEqualTo("19000");
        assertThat(properties.getProperty("database.hostname")).isEqualTo("db.internal");
        assertThat(properties.getProperty("database.dbname")).isEqualTo("inventory");
        assertThat(properties.getProperty("schema.include.list")).isEqualTo("analytics");
        assertThat(properties.getProperty("table.include.list")).isEqualTo("analytics.events");
        assertThat(properties.getProperty("snapshot.mode")).isEqualTo("never");
    }

    private static Properties extractProperties(DebeziumSourceFunction<?> sourceFunction)
            throws Exception {
        Field propertiesField = DebeziumSourceFunction.class.getDeclaredField("properties");
        propertiesField.setAccessible(true);
        return (Properties) propertiesField.get(sourceFunction);
    }

    private static final class TestingDeserializationSchema<T>
            implements DebeziumDeserializationSchema<T> {
        @Override
        public void deserialize(SourceRecord record, Collector<T> out) {}

        @SuppressWarnings("unchecked")
        @Override
        public TypeInformation<T> getProducedType() {
            return (TypeInformation<T>) TypeInformation.of(Object.class);
        }
    }
}
