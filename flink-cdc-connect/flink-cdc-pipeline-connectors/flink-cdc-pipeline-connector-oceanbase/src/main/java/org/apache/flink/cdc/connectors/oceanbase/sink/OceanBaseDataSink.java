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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;

import java.io.Serializable;
import java.time.ZoneId;

/** A {@link DataSink} for "OceanBase" pipeline connector. */
public class OceanBaseDataSink implements DataSink, Serializable {

    private final OceanBaseConnectorOptions connectorOptions;

    private final ZoneId zoneId;

    public OceanBaseDataSink(OceanBaseConnectorOptions options, ZoneId zoneId) {
        this.connectorOptions = options;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        OceanBaseRecordFlusher recordFlusher =
                new OceanBaseRecordFlusher(connectorOptions, connectionProvider);
        return FlinkSinkProvider.of(
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseEventSerializationSchema(zoneId),
                        DataChangeRecord.KeyExtractor.simple(),
                        recordFlusher));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new OceanBaseMetadataApplier(connectorOptions);
    }
}
