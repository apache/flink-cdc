/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oceanbase.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/** Emits a row with physical fields and metadata fields. */
@Internal
public class OceanBaseAppendMetadataCollector implements Collector<RowData>, Serializable {
    private static final long serialVersionUID = 1L;

    private final OceanBaseMetadataConverter[] metadataConverters;

    public transient OceanBaseRecord inputRecord;
    public transient Collector<RowData> outputCollector;

    public OceanBaseAppendMetadataCollector(OceanBaseMetadataConverter[] metadataConverters) {
        this.metadataConverters = metadataConverters;
    }

    @Override
    public void collect(RowData physicalRow) {
        GenericRowData metaRow = new GenericRowData(metadataConverters.length);
        for (int i = 0; i < metadataConverters.length; i++) {
            Object meta = metadataConverters[i].read(inputRecord);
            metaRow.setField(i, meta);
        }
        RowData outRow = new JoinedRowData(physicalRow.getRowKind(), physicalRow, metaRow);
        outputCollector.collect(outRow);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
