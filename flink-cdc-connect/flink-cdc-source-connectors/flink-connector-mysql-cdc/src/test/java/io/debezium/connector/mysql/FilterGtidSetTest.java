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

import io.debezium.connector.mysql.MySqlConnectorConfig.GtidNewChannelPosition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Integration test for {@link MySqlStreamingChangeEventSource#filterGtidSet} to ensure the LATEST
 * mode fix (FLINK-39149) cannot regress.
 */
class FilterGtidSetTest {

    @Test
    void testFilterGtidSetLatestModeFixesNonContiguousGtid() throws Exception {
        MySqlStreamingChangeEventSource source =
                createSourceWithConfig(GtidNewChannelPosition.LATEST, null);

        MySqlOffsetContext offsetContext = createOffsetContext("aaa-111:5000-8000");
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000,bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result.toString()).contains("aaa-111:1-8000");
        assertThat(result.toString()).contains("bbb-222:1-3000");
    }

    @Test
    void testFilterGtidSetLatestModeWithSourceFilter() throws Exception {
        Predicate<String> excludeCcc = uuid -> !uuid.equals("ccc-333");
        MySqlStreamingChangeEventSource source =
                createSourceWithConfig(GtidNewChannelPosition.LATEST, excludeCcc);

        MySqlOffsetContext offsetContext = createOffsetContext("aaa-111:5000-8000,bbb-222:1-2000");
        GtidSet availableServerGtidSet =
                new GtidSet("aaa-111:1-10000,bbb-222:1-3000,ccc-333:1-5000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result.toString()).contains("aaa-111:1-8000");
        assertThat(result.toString()).contains("bbb-222:1-2000");
        assertThat(result.toString()).doesNotContain("ccc-333");
    }

    @Test
    void testFilterGtidSetEarliestModeNotAffected() throws Exception {
        MySqlStreamingChangeEventSource source =
                createSourceWithConfig(GtidNewChannelPosition.EARLIEST, null);

        MySqlOffsetContext offsetContext = createOffsetContext("aaa-111:5000-8000");
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000,bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result.toString()).contains("aaa-111:1-8000");
        assertThat(result.forServerWithId("bbb-222")).isNull();
    }

    @Test
    void testFilterGtidSetReturnsNullWhenNoGtid() throws Exception {
        MySqlStreamingChangeEventSource source =
                createSourceWithConfig(GtidNewChannelPosition.LATEST, null);

        MySqlOffsetContext offsetContext = createOffsetContext(null);
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:1-10000");
        GtidSet purgedServerGtid = new GtidSet("");

        GtidSet result =
                source.filterGtidSet(offsetContext, availableServerGtidSet, purgedServerGtid);

        assertThat(result).isNull();
    }

    private static MySqlStreamingChangeEventSource createSourceWithConfig(
            GtidNewChannelPosition channelPosition, Predicate<String> gtidSourceFilter)
            throws Exception {
        MySqlConnectorConfig mockConfig = Mockito.mock(MySqlConnectorConfig.class);
        when(mockConfig.gtidNewChannelPosition()).thenReturn(channelPosition);
        when(mockConfig.gtidSourceFilter()).thenReturn(gtidSourceFilter);

        MySqlStreamingChangeEventSource source =
                Mockito.mock(MySqlStreamingChangeEventSource.class, Mockito.CALLS_REAL_METHODS);

        Field configField =
                MySqlStreamingChangeEventSource.class.getDeclaredField("connectorConfig");
        configField.setAccessible(true);
        configField.set(source, mockConfig);

        return source;
    }

    private static MySqlOffsetContext createOffsetContext(String gtidSetStr) {
        MySqlOffsetContext offsetContext = Mockito.mock(MySqlOffsetContext.class);
        when(offsetContext.gtidSet()).thenReturn(gtidSetStr);
        return offsetContext;
    }
}
