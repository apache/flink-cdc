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

package io.debezium.connector.tidb;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

@ThreadSafe
public class TidbTopicSelector {

    /**
     * Get the default topic selector logic, which uses a '.' delimiter character when needed.
     *
     * @param prefix the name of the prefix to be used for all topics; may not be null and must not
     *     terminate in the {@code delimiter}
     * @param heartbeatPrefix the name of the prefix to be used for all heartbeat topics; may not be
     *     null and must not terminate in the {@code delimiter}
     * @return the topic selector; never null
     */
    @Deprecated
    public static TopicSelector<TableId> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(
                prefix,
                heartbeatPrefix,
                ".",
                (t, pref, delimiter) -> String.join(delimiter, pref, t.catalog(), t.table()));
    }

    public static TopicSelector<TableId> defaultSelector(TiDBConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(
                connectorConfig,
                (tableId, prefix, delimiter) ->
                        String.join(delimiter, prefix, tableId.catalog(), tableId.table()));
    }
}
