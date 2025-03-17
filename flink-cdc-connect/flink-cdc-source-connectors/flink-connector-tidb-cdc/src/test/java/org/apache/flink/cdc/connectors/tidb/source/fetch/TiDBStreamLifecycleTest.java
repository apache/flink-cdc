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

package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests realtime and idempotent TiDB stream cleanup. */
class TiDBStreamLifecycleTest {

    @Test
    void shouldStopReaderContextWhenClosedRepeatedly() {
        EventSourceReader reader = createReader();
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        reader.context = context;

        reader.close();
        reader.close();

        assertThat(context.isRunning()).isFalse();
        assertThat(reader.isClosed()).isTrue();
    }

    @Test
    void shouldAllowClosingTaskBeforeItStarts() {
        TiDBStreamFetchTask task = new TiDBStreamFetchTask(createStreamSplit());

        task.close();
        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    private EventSourceReader createReader() {
        TiDBSourceConfigFactory configFactory = new TiDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(4000);
        configFactory.username("root");
        configFactory.password("");
        configFactory.databaseList("inventory");
        configFactory.tableList("inventory.products");
        configFactory.pdAddresses("localhost:2379");
        TiDBSourceConfig sourceConfig = configFactory.create(0);
        return new EventSourceReader(
                sourceConfig.getDbzConnectorConfig(), null, null, null, createStreamSplit());
    }

    private StreamSplit createStreamSplit() {
        return new StreamSplit(
                "stream-split",
                EventOffset.INITIAL_OFFSET,
                EventOffset.NO_STOPPING_OFFSET,
                Collections.emptyList(),
                Collections.singletonMap(new TableId("inventory", null, "products"), null),
                0);
    }
}
