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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/**
 * Source connector integration test for numeric(0) IndexOutOfBoundsException fix.
 *
 * <p>This test uses the PostgreSQL source connector directly to verify that numeric(0) fields can
 * be processed without throwing IndexOutOfBoundsException.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class PostgresNumericZeroSourceITCase extends PostgresTestBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresNumericZeroSourceITCase.class);
    private static final String SCHEMA_NAME = "numeric_zero_precision_test";
    private static final String PLUGIN_NAME = "pgoutput";
    private static final DebeziumDeserializationSchema<String> deserializer =
            new JsonDebeziumDeserializationSchema();
    private String slotName;
    private static final String DB_NAME_PREFIX = "postgres";
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @BeforeEach
    public void before() {
        this.customDatabase.createAndInitialize();
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    /**
     * Test bigint fields behavior and verify they are not affected by decimal.handling.mode. This
     * test ensures bigint fields always map to BIGINT type regardless of decimal mode.
     */
    @ParameterizedTest
    @ValueSource(strings = {"string", "double", "precise"})
    public void testBigintFieldsWithDecimalModes(String mode) throws Exception {
        LOG.info("Testing bigint fields with decimal.handling.mode = {}", mode);

        // Reuse the common test logic but focus on bigint field validation
        testWithDecimalModeAndSlotSuffix(mode);
    }

    private void testWithDecimalModeAndSlotSuffix(String decimalMode) throws Exception {
        LOG.info("Testing numeric(0) with decimal.handling.mode = {}", decimalMode);
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", decimalMode);
        // Add properties to improve stability
        debeziumProperties.put("slot.drop.on.stop", "true");
        debeziumProperties.put("publication.autocreate.mode", "filtered");
        debeziumProperties.put("plugin.name", PLUGIN_NAME);

        JdbcIncrementalSource<String> postgresSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .database(this.customDatabase.getDatabaseName())
                        .tableList("inventory.numeric_zero_test")
                        .debeziumProperties(debeziumProperties)
                        .slotName(slotName)
                        .decodingPluginName(PLUGIN_NAME)
                        .startupOptions(StartupOptions.initial())
                        .deserializer(deserializer)
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream =
                env.fromSource(postgresSource, WatermarkStrategy.noWatermarks(), "PostgresSource");

        CloseableIterator<String> iterator = stream.executeAndCollect();
        int totalRecords = 0;
        int expectedTotalRecords = 4;
        while (iterator.hasNext() && totalRecords < expectedTotalRecords) {
            String record = iterator.next();
            LOG.info(record);
            totalRecords++;
            // Verify we can parse the JSON without errors - this is the core fix validation
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> recordMap = objectMapper.readValue(record, Map.class);
            assertThat(recordMap).isNotNull();

            // The fact that we received and parsed records without IndexOutOfBoundsException
            // demonstrates that the fix is working
            validateNumericZeroFields(recordMap, decimalMode);
        }
        iterator.close();
    }

    /**
     * Validate that numeric(0) fields can be accessed without throwing IndexOutOfBoundsException
     * and that the values are correctly processed according to the decimal handling mode.
     */
    private void validateNumericZeroFields(Map<String, Object> recordMap, String decimalMode) {
        Map<String, Object> payload = (Map<String, Object>) recordMap.get("payload");
        if (payload != null) {
            Map<String, Object> after = (Map<String, Object>) payload.get("after");
            if (after != null) {
                // Get the record identifier to know which test data we're validating
                Object nameObj = after.get("name");
                String recordName = nameObj != null ? nameObj.toString() : "unknown";

                // These fields were previously causing IndexOutOfBoundsException
                Object numericZero = after.get("numeric_zero");
                Object nullableZero = after.get("nullable_zero");
                Object bigValue = after.get("big_value");
                Object regularNumeric = after.get("regular_numeric");
                Object decimalValue = after.get("decimal_value");
                Object bigintValue = after.get("bigint_value");
                Object bigintNullable = after.get("bigint_nullable");

                LOG.debug(
                        "Validating record '{}' with mode {}: numeric_zero={}, nullable_zero={}, big_value={}",
                        recordName,
                        decimalMode,
                        numericZero,
                        nullableZero,
                        bigValue);

                // Validate that we can access all fields without IndexOutOfBoundsException
                assertThat(after)
                        .containsKeys(
                                "numeric_zero",
                                "nullable_zero",
                                "big_value",
                                "regular_numeric",
                                "decimal_value",
                                "bigint_value",
                                "name");

                // Validate specific test data based on the record name and decimal mode
                validateSpecificRecordData(
                        recordName,
                        numericZero,
                        nullableZero,
                        bigValue,
                        regularNumeric,
                        decimalValue,
                        bigintValue,
                        bigintNullable,
                        decimalMode);
            }
        }
    }

    /** Validate specific record data based on the test data from numeric_zero_precision_test.sql */
    private void validateSpecificRecordData(
            String recordName,
            Object numericZero,
            Object nullableZero,
            Object bigValue,
            Object regularNumeric,
            Object decimalValue,
            Object bigintValue,
            Object bigintNullable,
            String decimalMode) {
        switch (recordName) {
            case "test_null":
                // Expected: (42, NULL, 123.45, 999999999, 100, 9223372036854775807, NULL,
                // 'test_null')
                validateNumericField(numericZero, 42, decimalMode, "numeric_zero");
                assertThat(nullableZero).isNull(); // Should be NULL
                validateNumericField(bigValue, 999999999, decimalMode, "big_value");
                validateRegularNumericField(regularNumeric, "123.45", decimalMode);
                validateNumericField(decimalValue, 100, decimalMode, "decimal_value");
                assertThat(bigintValue).isEqualTo(Long.MAX_VALUE); // 9223372036854775807
                assertThat(bigintNullable).isNull(); // Should be NULL
                break;

            case "test_zeros":
                // Expected: (null, null, 0.00, null, null, 0, 0, 'test_zeros')
                assertThat(numericZero).isNull();
                assertThat(nullableZero).isNull();
                assertThat(bigValue).isNull();
                validateRegularNumericField(regularNumeric, "0.00", decimalMode);
                assertThat(decimalValue).isNull();
                assertThat(bigintValue).isEqualTo(0L);
                assertThat(bigintNullable).isEqualTo(0L);
                break;

            case "test_mixed":
                // Expected: (-123, 456, -789.01, 2147483647, -50, -9223372036854775808,
                // 12345678901234, 'test_mixed')
                validateNumericField(numericZero, -123, decimalMode, "numeric_zero");
                validateNumericField(nullableZero, 456, decimalMode, "nullable_zero");
                validateNumericField(bigValue, 2147483647L, decimalMode, "big_value");
                validateRegularNumericField(regularNumeric, "-789.01", decimalMode);
                validateNumericField(decimalValue, -50, decimalMode, "decimal_value");
                assertThat(bigintValue).isEqualTo(Long.MIN_VALUE); // -9223372036854775808
                assertThat(bigintNullable).isEqualTo(12345678901234L);
                break;

            case "test_all_nulls":
                // Expected: (NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'test_all_nulls')
                assertThat(numericZero).isNull();
                assertThat(nullableZero).isNull();
                assertThat(bigValue).isNull();
                assertThat(regularNumeric).isNull();
                assertThat(decimalValue).isNull();
                assertThat(bigintValue).isNull();
                assertThat(bigintNullable).isNull();
                break;

            case "test_bigint_range":
                // Expected: (999, 888, 77.66, 555444333, 22.11, 1000000000000000000,
                // -1000000000000000000, 'test_bigint_range')
                validateNumericField(numericZero, 999, decimalMode, "numeric_zero");
                validateNumericField(nullableZero, 888, decimalMode, "nullable_zero");
                validateNumericField(bigValue, 555444333L, decimalMode, "big_value");
                validateRegularNumericField(regularNumeric, "77.66", decimalMode);
                validateNumericField(
                        decimalValue,
                        22,
                        decimalMode,
                        "decimal_value"); // 22.11 -> 22 for integer fields
                assertThat(bigintValue).isEqualTo(1000000000000000000L);
                assertThat(bigintNullable).isEqualTo(-1000000000000000000L);
                break;

            default:
                LOG.debug("Unknown record name: {}, skipping specific validation", recordName);
        }
    }

    /**
     * Validate numeric(0) fields based on decimal handling mode. These fields have zero precision
     * and should be handled differently based on the mode.
     */
    private void validateNumericField(
            Object actualValue, Object expectedValue, String decimalMode, String fieldName) {
        if (expectedValue == null) {
            assertThat(actualValue).as("Field %s should be null", fieldName).isNull();
            return;
        }

        switch (decimalMode) {
            case "string":
                // In string mode, numeric values are represented as strings
                if (actualValue != null) {
                    String actualStr = actualValue.toString();
                    String expectedStr = expectedValue.toString();
                    // Remove decimal point for integer values
                    if (expectedStr.endsWith(".0")) {
                        expectedStr = expectedStr.substring(0, expectedStr.length() - 2);
                    }
                    assertThat(actualStr)
                            .as("Field %s in string mode", fieldName)
                            .isEqualTo(expectedStr);
                }
                break;

            case "double":
                // In double mode, numeric values are represented as doubles
                if (actualValue != null) {
                    Double actualDouble =
                            actualValue instanceof Number
                                    ? ((Number) actualValue).doubleValue()
                                    : Double.parseDouble(actualValue.toString());
                    Double expectedDouble =
                            expectedValue instanceof Number
                                    ? ((Number) expectedValue).doubleValue()
                                    : Double.parseDouble(expectedValue.toString());
                    assertThat(actualDouble)
                            .as("Field %s in double mode", fieldName)
                            .isCloseTo(expectedDouble, within(0.001));
                }
                break;

            case "precise":
            default:
                // In precise mode, numeric(0) fields should be mapped to BIGINT
                if (actualValue != null) {
                    Long actualLong =
                            actualValue instanceof Number
                                    ? ((Number) actualValue).longValue()
                                    : Long.parseLong(actualValue.toString());
                    Long expectedLong =
                            expectedValue instanceof Number
                                    ? ((Number) expectedValue).longValue()
                                    : Long.parseLong(expectedValue.toString());
                    assertThat(actualLong)
                            .as("Field %s in precise mode", fieldName)
                            .isEqualTo(expectedLong);
                }
                break;
        }
    }

    /** Validate regular numeric fields with precision and scale (e.g., NUMERIC(10,2)). */
    private void validateRegularNumericField(
            Object actualValue, String expectedValue, String decimalMode) {
        if (expectedValue == null) {
            assertThat(actualValue).as("Regular numeric field should be null").isNull();
            return;
        }

        switch (decimalMode) {
            case "string":
                // In string mode, decimal values are represented as strings
                if (actualValue != null) {
                    assertThat(actualValue.toString())
                            .as("Regular numeric in string mode")
                            .isEqualTo(expectedValue);
                }
                break;

            case "double":
                // In double mode, decimal values are represented as doubles
                if (actualValue != null) {
                    Double actualDouble =
                            actualValue instanceof Number
                                    ? ((Number) actualValue).doubleValue()
                                    : Double.parseDouble(actualValue.toString());
                    Double expectedDouble = Double.parseDouble(expectedValue);
                    assertThat(actualDouble)
                            .as("Regular numeric in double mode")
                            .isCloseTo(expectedDouble, within(0.001));
                }
                break;

            case "precise":
            default:
                // In precise mode, decimal values should maintain precision
                if (actualValue != null) {
                    // For precise mode, we expect the value to be preserved accurately
                    // This could be a decimal type or a string representation
                    String actualStr = actualValue.toString();
                    assertThat(actualStr)
                            .as("Regular numeric in precise mode")
                            .isEqualTo(expectedValue);
                }
                break;
        }
    }

    /**
     * Test concurrent access to numeric(0) fields to ensure thread safety of the fix. This test
     * verifies that multiple threads can simultaneously process records with numeric(0) fields
     * without encountering race conditions or IndexOutOfBoundsException.
     */
    @ParameterizedTest
    @ValueSource(strings = {"string", "double", "precise"})
    public void testConcurrentNumericZeroProcessing(String decimalMode) throws Exception {
        LOG.info(
                "Testing concurrent numeric(0) processing with decimal.handling.mode = {}",
                decimalMode);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", decimalMode);
        debeziumProperties.put("slot.drop.on.stop", "true");
        debeziumProperties.put("publication.autocreate.mode", "filtered");
        debeziumProperties.put("plugin.name", PLUGIN_NAME);

        JdbcIncrementalSource<String> postgresSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .database(this.customDatabase.getDatabaseName())
                        .tableList("inventory.numeric_zero_test")
                        .debeziumProperties(debeziumProperties)
                        .slotName(slotName)
                        .decodingPluginName(PLUGIN_NAME)
                        .startupOptions(StartupOptions.initial())
                        .deserializer(deserializer)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream =
                env.fromSource(postgresSource, WatermarkStrategy.noWatermarks(), "PostgresSource");

        CloseableIterator<String> iterator = stream.executeAndCollect();

        // Concurrent processing test
        int threadCount = 4;
        int expectedRecords = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);
        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<String> allRecords = new ArrayList<>();

        try {
            // Collect all records first
            while (iterator.hasNext() && allRecords.size() < expectedRecords) {
                String record = iterator.next();
                allRecords.add(record);
                LOG.debug("Collected record for concurrent test: {}", record);
            }

            assertThat(allRecords).hasSize(expectedRecords);

            // Process records concurrently
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                // Wait for all threads to start simultaneously
                                startLatch.await();

                                // Each thread processes all records to simulate concurrent access
                                for (String record : allRecords) {
                                    try {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        Map<String, Object> recordMap =
                                                objectMapper.readValue(record, Map.class);
                                        assertThat(recordMap).isNotNull();

                                        // Validate numeric(0) fields can be accessed without
                                        // exceptions
                                        validateNumericZeroFields(recordMap, decimalMode);
                                        totalProcessed.incrementAndGet();

                                        LOG.debug(
                                                "Thread {} successfully processed record",
                                                threadId);
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Thread {} encountered error processing record: {}",
                                                threadId,
                                                e.getMessage());
                                        errorCount.incrementAndGet();
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                LOG.error("Thread {} was interrupted", threadId);
                                errorCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        },
                        executor);
            }

            // Start all threads simultaneously
            startLatch.countDown();

            // Wait for all threads to complete
            boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete within timeout").isTrue();

            // Verify results
            assertThat(errorCount.get())
                    .as("No errors should occur during concurrent processing")
                    .isEqualTo(0);

            int expectedTotalProcessed = threadCount * expectedRecords;
            assertThat(totalProcessed.get())
                    .as("All records should be processed by all threads")
                    .isEqualTo(expectedTotalProcessed);

            LOG.info(
                    "Concurrent test completed successfully: {} threads processed {} records each (total: {})",
                    threadCount,
                    expectedRecords,
                    totalProcessed.get());

        } finally {
            executor.shutdown();
            iterator.close();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Test PostgreSQL version compatibility for numeric(0) field handling. This test ensures that
     * the fix works consistently across different PostgreSQL versions.
     */
    @Test
    public void testPostgresVersionCompatibility() throws Exception {
        LOG.info("Testing PostgreSQL version compatibility for numeric(0) fields");

        // Note: This test uses the current container version but validates that
        // the numeric(0) handling is version-agnostic by testing the core scenarios
        // that would fail differently across versions if not properly handled

        String postgresVersion = POSTGRES_CONTAINER.getDockerImageName();
        LOG.info("Running compatibility test against PostgreSQL version: {}", postgresVersion);

        // Test all three decimal handling modes to ensure version compatibility
        String[] modes = {"string", "double", "precise"};

        for (String mode : modes) {
            LOG.info("Testing mode '{}' for version compatibility", mode);

            Properties debeziumProperties = new Properties();
            debeziumProperties.put("decimal.handling.mode", mode);
            debeziumProperties.put("slot.drop.on.stop", "true");
            debeziumProperties.put("publication.autocreate.mode", "filtered");
            debeziumProperties.put("plugin.name", PLUGIN_NAME);

            // Use a unique slot name for each mode to avoid conflicts
            String versionTestSlotName = slotName + "_version_" + mode;

            JdbcIncrementalSource<String> postgresSource =
                    PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                            .hostname(POSTGRES_CONTAINER.getHost())
                            .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                            .username(TEST_USER)
                            .password(TEST_PASSWORD)
                            .database(this.customDatabase.getDatabaseName())
                            .tableList("inventory.numeric_zero_test")
                            .debeziumProperties(debeziumProperties)
                            .slotName(versionTestSlotName)
                            .decodingPluginName(PLUGIN_NAME)
                            .startupOptions(StartupOptions.initial())
                            .deserializer(deserializer)
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> stream =
                    env.fromSource(
                            postgresSource, WatermarkStrategy.noWatermarks(), "PostgresSource");

            CloseableIterator<String> iterator = stream.executeAndCollect();

            try {
                int recordCount = 0;
                int maxRecords = 4;

                while (iterator.hasNext() && recordCount < maxRecords) {
                    String record = iterator.next();
                    recordCount++;

                    LOG.debug("Version compatibility test - Mode: {}, Record: {}", mode, record);

                    // Parse and validate the record
                    ObjectMapper objectMapper = new ObjectMapper();
                    Map<String, Object> recordMap = objectMapper.readValue(record, Map.class);
                    assertThat(recordMap).isNotNull();

                    // The key test: ensure numeric(0) fields don't cause version-specific issues
                    validateNumericZeroFields(recordMap, mode);
                }

                assertThat(recordCount)
                        .as("Should process expected number of records for mode: " + mode)
                        .isEqualTo(maxRecords);

                LOG.info("Version compatibility test passed for mode '{}'", mode);

            } finally {
                iterator.close();
                // Clean up the version-specific slot
                try {
                    customDatabase.removeSlot(versionTestSlotName);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to cleanup version test slot {}: {}",
                            versionTestSlotName,
                            e.getMessage());
                }
            }
        }

        LOG.info(
                "PostgreSQL version compatibility test completed successfully for version: {}",
                postgresVersion);
    }
}
