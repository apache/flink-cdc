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
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.dws.sink.v2.DwsSink;

import java.io.Serializable;

/** A {@link DataSink} for the GaussDB DWS pipeline connector. */
public class DwsDataSink implements DataSink, Serializable {

    private final DwsDataSinkConfig sinkConfig;

    public DwsDataSink(DwsDataSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new DwsSink(sinkConfig));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DwsMetadataApplier(
                sinkConfig.getUrl(),
                sinkConfig.getUsername(),
                sinkConfig.getPassword(),
                sinkConfig.isCaseSensitive(),
                sinkConfig.getDefaultSchema(),
                sinkConfig.isEnableDnPartition(),
                sinkConfig.getDistributionKey());
    }

    @Override
    public boolean requiresPrimaryKeyUpdateSplit() {
        return true;
    }
}
