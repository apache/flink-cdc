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

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussSink;

import com.alibaba.fluss.config.Configuration;

import java.util.List;
import java.util.Map;

/** A DataSink implementation for Fluss. */
public class FlussDataSink implements DataSink {

    private final Configuration flussClientConfig;
    private final Map<String, String> tableProperties;
    private final Map<String, List<String>> bucketKeysMap;
    private final Map<String, Integer> bucketNumMap;

    public FlussDataSink(
            Configuration flussClientConfig,
            Map<String, String> tableProperties,
            Map<String, List<String>> bucketKeysMap,
            Map<String, Integer> bucketNumMap) {
        this.flussClientConfig = flussClientConfig;
        this.tableProperties = tableProperties;
        this.bucketKeysMap = bucketKeysMap;
        this.bucketNumMap = bucketNumMap;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(
                new FlussSink<>(flussClientConfig, new FlussEventSerializationSchema()));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new FlussMetaDataApplier(
                flussClientConfig, tableProperties, bucketKeysMap, bucketNumMap);
    }

    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return new FlussHashFunctionProvider();
    }
}
