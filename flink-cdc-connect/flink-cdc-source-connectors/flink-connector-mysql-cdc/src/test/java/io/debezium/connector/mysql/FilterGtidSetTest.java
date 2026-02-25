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

package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.GtidNewChannelPosition;
import io.debezium.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link MySqlStreamingChangeEventSource#filterGtidSet} to ensure the LATEST
 * mode fix (FLINK-39149) cannot regress. This test directly invokes the production method via
 * reflection, bypassing the heavy constructor dependencies.
 */
class FilterGtidSetTest {

    /**
     * Verifies that filterGtidSet() in LATEST mode fixes non-contiguous checkpoint GTIDs for old
     * channels and uses server's full GTID for new channels.
     *
     * <p>This is the core FLINK-39149 bug scenario: checkpoint has "aaa-111:5000-8000" (gap
     * 1-4999), and without the fix, MySQL would replay transactions 1-4999.
     */
    @Test
    void testFilterGtidSetLatestModeFixesNonContiguousGtid() throws Exception {
        MySqlStreamingChangeEventSource source = createSourceWithConfig("latest", null);

        MySqlOffsetContext offsetContext = createOffsetContext(source, "aaa-111:5000-8000");
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000,bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        // Old channel aaa-111: gap should be filled from :1 to 8000
        assertThat(result.toString()).contains("aaa-111:1-8000");
        // New channel bbb-222: should use server's full GTID
        assertThat(result.toString()).contains("bbb-222:1-3000");
    }

    /**
     * Verifies that filterGtidSet() in LATEST mode with gtidSourceFilter excludes filtered UUIDs.
     */
    @Test
    void testFilterGtidSetLatestModeWithSourceFilter() throws Exception {
        MySqlStreamingChangeEventSource source = createSourceWithConfig("latest", "ccc-333");

        MySqlOffsetContext offsetContext =
                createOffsetContext(source, "aaa-111:5000-8000,bbb-222:1-2000");
        GtidSet availableServerGtidSet =
                new GtidSet("aaa-111:1-10000,bbb-222:1-3000,ccc-333:1-5000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result.toString()).contains("aaa-111:1-8000");
        assertThat(result.toString()).contains("bbb-222:1-2000");
        // ccc-333 should be excluded
        assertThat(result.toString()).doesNotContain("ccc-333");
    }

    /**
     * Verifies that filterGtidSet() in EARLIEST mode still works correctly (no regression from
     * LATEST mode fix).
     */
    @Test
    void testFilterGtidSetEarliestModeNotAffected() throws Exception {
        MySqlStreamingChangeEventSource source = createSourceWithConfig("earliest", null);

        MySqlOffsetContext offsetContext = createOffsetContext(source, "aaa-111:5000-8000");
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000,bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        // EARLIEST mode: old channel should be fixed
        assertThat(result.toString()).contains("aaa-111:1-8000");
        // EARLIEST mode does NOT add new channels (different from LATEST)
        assertThat(result.forServerWithId("bbb-222")).isNull();
    }

    /** Verifies that filterGtidSet() returns null when offsetContext has no GTID. */
    @Test
    void testFilterGtidSetReturnsNullWhenNoGtid() throws Exception {
        MySqlStreamingChangeEventSource source = createSourceWithConfig("latest", null);

        MySqlOffsetContext offsetContext = createOffsetContext(source, null);
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result).isNull();
    }

    // ---- Helper methods ----

    /**
     * Creates a MySqlStreamingChangeEventSource via Unsafe (bypassing constructor) and injects the
     * connectorConfig field via reflection.
     */
    @SuppressWarnings("restriction")
    private static MySqlStreamingChangeEventSource createSourceWithConfig(
            String gtidNewChannelPosition, String gtidSourceExcludes) throws Exception {
        // Build Debezium Configuration
        JdbcConfiguration.Builder builder =
                JdbcConfiguration.create().with("database.server.name", "test_server");
        if (gtidNewChannelPosition != null) {
            builder = builder.with("gtid.new.channel.position", gtidNewChannelPosition);
        }
        if (gtidSourceExcludes != null) {
            builder = builder.with("gtid.source.excludes", gtidSourceExcludes);
        }
        Configuration dezConf = builder.build();
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dezConf);

        // Verify config is as expected
        if ("latest".equalsIgnoreCase(gtidNewChannelPosition)) {
            assertThat(connectorConfig.gtidNewChannelPosition())
                    .isEqualTo(GtidNewChannelPosition.LATEST);
        } else if ("earliest".equalsIgnoreCase(gtidNewChannelPosition)) {
            assertThat(connectorConfig.gtidNewChannelPosition())
                    .isEqualTo(GtidNewChannelPosition.EARLIEST);
        }

        // Create instance via Unsafe to bypass heavy constructor
        sun.misc.Unsafe unsafe = getUnsafe();
        MySqlStreamingChangeEventSource source =
                (MySqlStreamingChangeEventSource)
                        unsafe.allocateInstance(MySqlStreamingChangeEventSource.class);

        // Inject connectorConfig via reflection
        Field configField =
                MySqlStreamingChangeEventSource.class.getDeclaredField("connectorConfig");
        configField.setAccessible(true);
        configField.set(source, connectorConfig);

        return source;
    }

    private static MySqlOffsetContext createOffsetContext(
            MySqlStreamingChangeEventSource source, String gtidSetStr) throws Exception {
        Field configField =
                MySqlStreamingChangeEventSource.class.getDeclaredField("connectorConfig");
        configField.setAccessible(true);
        MySqlConnectorConfig config = (MySqlConnectorConfig) configField.get(source);

        if (gtidSetStr == null) {
            // Return an offset context without GTID (gtidSet() returns null)
            Map<String, Object> offsetMap = new HashMap<>();
            offsetMap.put("file", "mysql-bin.000001");
            offsetMap.put("pos", 4L);
            return new MySqlOffsetContext.Loader(config).load(offsetMap);
        }

        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("file", "mysql-bin.000001");
        offsetMap.put("pos", 4L);
        offsetMap.put("gtids", gtidSetStr);
        return new MySqlOffsetContext.Loader(config).load(offsetMap);
    }

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() throws Exception {
        Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (sun.misc.Unsafe) unsafeField.get(null);
    }
}
