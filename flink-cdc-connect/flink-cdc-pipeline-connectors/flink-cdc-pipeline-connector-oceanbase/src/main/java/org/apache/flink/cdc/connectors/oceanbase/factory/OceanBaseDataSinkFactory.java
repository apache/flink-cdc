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

package org.apache.flink.cdc.connectors.oceanbase.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.oceanbase.sink.OceanBaseDataSink;
import org.apache.flink.cdc.connectors.oceanbase.sink.OceanBaseDataSinkOptions;
import org.apache.flink.cdc.connectors.oceanbase.utils.OptionUtils;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;

/** A {@link DataSinkFactory} to create {@link OceanBaseDataSink}. */
@Internal
public class OceanBaseDataSinkFactory implements DataSinkFactory {

    private static final String IDENTIFIER = "oceanbase";

    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();
        Map<String, String> configMap = config.toMap();
        OptionUtils.printOptions(IDENTIFIER, configMap);

        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(configMap);
        String zoneStr = context.getPipelineConfiguration().get(PIPELINE_LOCAL_TIME_ZONE);
        ZoneId zoneId =
                PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zoneStr)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneStr);
        return new OceanBaseDataSink(connectorOptions, zoneId);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(OceanBaseDataSinkOptions.URL);
        requiredOptions.add(OceanBaseDataSinkOptions.USERNAME);
        requiredOptions.add(OceanBaseDataSinkOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(OceanBaseDataSinkOptions.DRIVER_CLASS_NAME);
        optionalOptions.add(OceanBaseDataSinkOptions.DRUID_PROPERTIES);
        optionalOptions.add(OceanBaseDataSinkOptions.MEMSTORE_CHECK_ENABLED);
        optionalOptions.add(OceanBaseDataSinkOptions.MEMSTORE_THRESHOLD);
        optionalOptions.add(OceanBaseDataSinkOptions.MEMSTORE_CHECK_INTERVAL);
        optionalOptions.add(OceanBaseDataSinkOptions.PARTITION_ENABLED);
        optionalOptions.add(OceanBaseDataSinkOptions.SYNC_WRITE);
        optionalOptions.add(OceanBaseDataSinkOptions.BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(OceanBaseDataSinkOptions.BUFFER_SIZE);
        optionalOptions.add(OceanBaseDataSinkOptions.MAX_RETRIES);
        return optionalOptions;
    }
}
