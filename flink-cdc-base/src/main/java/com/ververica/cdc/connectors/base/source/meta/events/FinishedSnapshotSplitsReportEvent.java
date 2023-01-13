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

package com.ververica.cdc.connectors.base.source.meta.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;

import java.util.Map;

/**
 * The {@link SourceEvent} that {@link IncrementalSourceReader} sends to {@link
 * IncrementalSourceEnumerator} to notify the snapshot split has read finished with the consistent
 * change log position.
 */
public class FinishedSnapshotSplitsReportEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Map<String, Offset> finishedOffsets;

    public FinishedSnapshotSplitsReportEvent(Map<String, Offset> finishedOffsets) {
        this.finishedOffsets = finishedOffsets;
    }

    public Map<String, Offset> getFinishedOffsets() {
        return finishedOffsets;
    }

    @Override
    public String toString() {
        return "FinishedSnapshotSplitsReportEvent{" + "finishedOffsets=" + finishedOffsets + '}';
    }
}
