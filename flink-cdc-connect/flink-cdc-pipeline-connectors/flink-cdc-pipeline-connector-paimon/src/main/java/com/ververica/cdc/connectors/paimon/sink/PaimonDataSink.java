/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.paimon.sink;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.connectors.paimon.sink.v2.PaimonSink;
import org.apache.paimon.options.Options;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/** A {@link DataSink} for Paimon connector that supports schema evolution. */
public class PaimonDataSink implements DataSink, Serializable {

    // options for creating Paimon catalog.
    private final Options options;

    // options for creating Paimon table.
    private final Map<String, String> tableOptions;

    private final ZoneId zoneId;

    private final String commitUser;

    private final Map<TableId, List<String>> partitionMaps;

    public PaimonDataSink(
            Options options,
            Map<String, String> tableOptions,
            ZoneId zoneId,
            String commitUser,
            Map<TableId, List<String>> partitionMaps) {
        this.options = options;
        this.tableOptions = tableOptions;
        this.zoneId = zoneId;
        this.commitUser = commitUser;
        this.partitionMaps = partitionMaps;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new PaimonSink(options, zoneId, commitUser));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new PaimonMetadataApplier(options, tableOptions, partitionMaps);
    }
}
