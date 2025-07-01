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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;

import com.alibaba.fluss.config.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.sink.FlussConfigUtils.parseBucketKeys;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussConfigUtils.parseBucketNumber;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BUCKET_KEY;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BUCKET_NUMBER;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.CLIENT_ID;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.PREFIX_FLUSS_PROPERTIES;

/** Factory for creating configured instances of {@link FlussDataSink}. */
public class FlussDataSinkFactory implements DataSinkFactory {
    public static final String IDENTIFIER = "fluss";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validateExcept(PREFIX_FLUSS_PROPERTIES);
        org.apache.flink.cdc.common.configuration.Configuration factoryConfiguration =
                context.getFactoryConfiguration();

        Map<String, String> flussOptions = new HashMap<>();
        factoryConfiguration
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(PREFIX_FLUSS_PROPERTIES)) {
                                flussOptions.put(
                                        key.substring(PREFIX_FLUSS_PROPERTIES.length()), value);
                            } else {
                                flussOptions.put(key, value);
                            }
                        });
        Configuration config = Configuration.fromMap(flussOptions);
        Map<String, List<String>> bucketKeysMap =
                parseBucketKeys(factoryConfiguration.get(BUCKET_KEY));
        Map<String, Integer> bucketNumMap =
                parseBucketNumber(factoryConfiguration.get(BUCKET_NUMBER));
        return new FlussDataSink(config, bucketKeysMap, bucketNumMap);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BUCKET_KEY);
        options.add(BUCKET_NUMBER);
        options.add(CLIENT_ID);
        return options;
    }
}
