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

package org.apache.flink.cdc.connectors.fluss.sink;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for parsing fluss yaml sink options. */
public class FlussConfigUtils {
    public static Map<String, List<String>> parseBucketKeys(@Nullable String rawValue)
            throws IllegalArgumentException {
        Map<String, List<String>> result = new HashMap<>();
        if (rawValue == null || rawValue.isEmpty()) {
            return result;
        }

        for (String tableBucketKeyStr : rawValue.split(";")) {
            if (tableBucketKeyStr.trim().isEmpty()) {
                continue;
            }

            String[] tableAndBucketKeys = tableBucketKeyStr.trim().split(":", 2);
            if (tableAndBucketKeys.length != 2) {
                throw new IllegalArgumentException("Invalid bucket key configuration: " + rawValue);
            }

            String table = tableAndBucketKeys[0].trim();
            List<String> keys = Arrays.asList(tableAndBucketKeys[1].trim().split(","));
            result.put(table, keys);
        }
        return result;
    }

    public static Map<String, Integer> parseBucketNumber(@Nullable String rawValue)
            throws IllegalArgumentException {
        Map<String, Integer> result = new HashMap<>();
        if (rawValue == null || rawValue.isEmpty()) {
            return result;
        }

        for (String tableBucketNumStr : rawValue.split(";")) {
            if (tableBucketNumStr.trim().isEmpty()) {
                continue;
            }
            String[] kv = tableBucketNumStr.trim().split(":", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid bucket number configuration: " + rawValue);
            }

            String table = kv[0].trim();
            try {
                int value = Integer.parseInt(kv[1].trim());
                result.put(table, value);
            } catch (NumberFormatException ignored) {
                throw new IllegalArgumentException(
                        "Invalid bucket number configuration: " + rawValue);
            }
        }
        return result;
    }
}
