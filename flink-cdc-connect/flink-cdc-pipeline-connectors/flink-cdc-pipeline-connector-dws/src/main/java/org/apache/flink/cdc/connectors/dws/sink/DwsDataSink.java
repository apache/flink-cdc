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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;

/** A {@link DataSink} for the GaussDB DWS pipeline connector. */
public class DwsDataSink implements DataSink, Serializable {

    private final DwsConnectionOptions connectorOptions;
    private final ZoneId zoneId;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final int autoFlushBatchSize;
    private final Duration autoFlushMaxInterval;
    private final boolean enableAutoFlush;
    private final boolean enableDelete;
    private final WriteMode writeMode;
    private final boolean enableDnPartition;
    private final String distributionKey;

    public DwsDataSink(
            DwsConnectionOptions connectorOptions,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            int autoFlushBatchSize,
            Duration autoFlushMaxInterval,
            boolean enableAutoFlush,
            boolean enableDelete,
            WriteMode writeMode,
            boolean enableDnPartition,
            String distributionKey) {
        this.connectorOptions = connectorOptions;
        this.zoneId = zoneId;
        this.caseSensitive = caseSensitive;
        this.defaultSchema = defaultSchema;
        this.autoFlushBatchSize = autoFlushBatchSize;
        this.autoFlushMaxInterval = autoFlushMaxInterval;
        this.enableAutoFlush = enableAutoFlush;
        this.enableDelete = enableDelete;
        this.writeMode = writeMode;
        this.enableDnPartition = enableDnPartition;
        this.distributionKey = distributionKey;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkFunctionProvider.of(
                new DwsSinkFunction(
                        connectorOptions,
                        zoneId,
                        caseSensitive,
                        defaultSchema,
                        autoFlushBatchSize,
                        autoFlushMaxInterval,
                        enableAutoFlush,
                        enableDelete,
                        writeMode));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DwsMetadataApplier(
                connectorOptions.getUrl(),
                connectorOptions.getUsername(),
                connectorOptions.getPassword(),
                caseSensitive,
                defaultSchema,
                enableDnPartition,
                distributionKey);
    }
}
