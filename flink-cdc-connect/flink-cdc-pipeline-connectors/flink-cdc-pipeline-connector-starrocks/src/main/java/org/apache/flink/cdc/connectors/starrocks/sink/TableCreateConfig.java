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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Configurations for creating a StarRocks table. See <a
 * href="https://docs.starrocks.io/docs/table_design/table_types/
 * primary_key_table/#create-a-table">StarRocks Documentation</a> for how to create a StarRocks
 * primary key table.
 */
public class TableCreateConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Number of buckets for the table. null if not set. */
    @Nullable private final Integer numBuckets;

    /** Properties for the table. */
    private final Map<String, String> properties;

    public TableCreateConfig(@Nullable Integer numBuckets, Map<String, String> properties) {
        this.numBuckets = numBuckets;
        this.properties = new HashMap<>(properties);
    }

    public Optional<Integer> getNumBuckets() {
        return numBuckets == null ? Optional.empty() : Optional.of(numBuckets);
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public static TableCreateConfig from(Configuration config) {
        Integer numBuckets = config.get(StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS);
        Map<String, String> tableProperties =
                config.toMap().entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey()
                                                .startsWith(
                                                        StarRocksDataSinkOptions
                                                                .TABLE_CREATE_PROPERTIES_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                entry.getKey()
                                                        .substring(
                                                                StarRocksDataSinkOptions
                                                                        .TABLE_CREATE_PROPERTIES_PREFIX
                                                                        .length())
                                                        .toLowerCase(),
                                        Map.Entry::getValue));
        return new TableCreateConfig(numBuckets, tableProperties);
    }
}
