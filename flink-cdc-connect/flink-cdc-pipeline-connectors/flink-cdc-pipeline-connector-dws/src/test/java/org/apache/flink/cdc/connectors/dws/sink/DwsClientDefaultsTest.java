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

package org.apache.flink.cdc.connectors.dws.sink;

import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.config.ConfigOp;
import com.huaweicloud.dws.client.config.DwsClientConfigs;
import com.huaweicloud.dws.client.model.WriteMode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Freezes the connector-owned safe defaults applied to the native DWS client. */
class DwsClientDefaultsTest {

    @Test
    void appliesFiniteSafeDefaultsAndSingleNativePartition() throws Exception {
        DwsConfig config = DwsClientConfigFactory.create(minimalSettings().build());

        assertThat(value(config, "WRITE_MODE")).isEqualTo(WriteMode.AUTO);
        assertThat(value(config, "WRITE_AUTO_FLUSH_BATCH_SIZE")).isEqualTo(30_000);
        assertThat(value(config, "WRITE_AUTO_FLUSH_MAX_INTERVAL")).isEqualTo(Duration.ofSeconds(3));
        assertThat(value(config, "WRITE_FORCE_FLUSH_BATCH_SIZE")).isEqualTo(40_000);

        assertThat(value(config, "JDBC_IS_DN")).isEqualTo(false);
        assertThat(value(config, "WRITE_PARTITION_POLICY").toString()).isEqualTo("DYNAMIC");
        assertThat(value(config, "WRITE_PARTITION_MIN")).isEqualTo(1);
        assertThat(value(config, "WRITE_PARTITION_MAX")).isEqualTo(1);

        assertThat(memoryBytes(value(config, "WRITE_BUFFER_ALL_MAX_BYTES")))
                .isEqualTo(128L * 1024 * 1024);
        assertThat(memoryBytes(value(config, "WRITE_BUFFER_TABLE_MAX_BYTES")))
                .isEqualTo(64L * 1024 * 1024);
        assertThat(memoryBytes(value(config, "WRITE_BUFFER_PARTITION_MAX_BYTES")))
                .isEqualTo(32L * 1024 * 1024);

        assertThat(value(config, "RETRY_MAX_TIMES")).isEqualTo(3);
        assertThat((Duration) value(config, "TIMEOUT_TASK")).isPositive();
        assertThat((Duration) value(config, "TIMEOUT_SQL_STATEMENT")).isPositive();
        assertThat(value(config, "WRITE_FORMAT_STRING_U0000")).isEqualTo(false);
        assertThat(value(config, "ENABLE_COPY_COMPATIBLE_ILLEGAL_CHARS")).isEqualTo(false);
    }

    @Test
    void preservesExplicitUpsertForceFlushAfterNativeClientConstructionRules() throws Exception {
        DwsConfig config =
                DwsClientConfigFactory.create(
                        minimalSettings()
                                .withWriteMode(WriteMode.UPSERT)
                                .withForceFlushBatchSize(52_000)
                                .build());

        assertThat(value(config, "WRITE_FORCE_FLUSH_BATCH_SIZE")).isEqualTo(52_000);
        assertThat(value(config, "WRITE_FORCE_FLUSH_UPSERT_BATCH_SIZE")).isEqualTo(52_000);
    }

    @Test
    void disablingAutomaticFlushStillKeepsFiniteForceProtection() throws Exception {
        DwsConfig config =
                DwsClientConfigFactory.create(
                        minimalSettings()
                                .withEnableAutoFlush(false)
                                .withForceFlushBatchSize(48_000)
                                .build());

        assertThat(value(config, "WRITE_AUTO_FLUSH_BATCH_SIZE")).isEqualTo(48_000);
        assertThat(value(config, "WRITE_AUTO_FLUSH_MAX_INTERVAL")).isEqualTo(Duration.ZERO);
        assertThat(value(config, "WRITE_FORCE_FLUSH_BATCH_SIZE"))
                .isEqualTo(48_000)
                .isNotEqualTo(Integer.MAX_VALUE);
    }

    private static DwsDataSinkConfig.Builder minimalSettings() {
        return DwsDataSinkConfig.builder()
                .withUrl("jdbc:gaussdb://localhost:8000/test")
                .withUsername("test-user")
                .withPassword("test-password");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object value(DwsConfig config, String fieldName) throws Exception {
        Field field = DwsClientConfigs.class.getField(fieldName);
        return config.get((ConfigOp) field.get(null));
    }

    private static long memoryBytes(Object memory) throws Exception {
        Method getter = memory.getClass().getMethod("getByteSize");
        return ((Number) getter.invoke(memory)).longValue();
    }
}
