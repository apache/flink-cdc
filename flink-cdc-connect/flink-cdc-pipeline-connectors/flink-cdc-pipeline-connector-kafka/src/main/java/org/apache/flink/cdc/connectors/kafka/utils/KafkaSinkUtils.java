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

package org.apache.flink.cdc.connectors.kafka.utils;

import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.DELIMITER_SELECTOR_TOPIC;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.DELIMITER_TABLE_MAPPINGS;

/** Util class for {@link org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSink}. */
public class KafkaSinkUtils {

    /** Parse the mapping text to a map from Selectors to Kafka Topic name. */
    public static Map<Selectors, String> parseSelectorsToTopicMap(String mappingRuleString) {
        // Keep the order.
        Map<Selectors, String> result = new LinkedHashMap<>();
        if (mappingRuleString == null || mappingRuleString.isEmpty()) {
            return result;
        }
        for (String mapping : mappingRuleString.split(DELIMITER_TABLE_MAPPINGS)) {
            String[] selectorsAndTopic = mapping.split(DELIMITER_SELECTOR_TOPIC);
            Preconditions.checkArgument(
                    selectorsAndTopic.length == 2,
                    "Please check your configuration of "
                            + KafkaDataSinkOptions.SINK_TABLE_ID_TO_TOPIC_MAPPING);
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(selectorsAndTopic[0]).build();
            result.put(selectors, selectorsAndTopic[1]);
        }
        return result;
    }
}
