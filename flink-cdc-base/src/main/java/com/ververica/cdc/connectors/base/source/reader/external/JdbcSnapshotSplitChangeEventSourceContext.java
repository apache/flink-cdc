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

package com.ververica.cdc.connectors.base.source.reader.external;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import io.debezium.pipeline.source.spi.ChangeEventSource;

/**
 * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high watermark
 * for each {@link SnapshotSplit}.
 */
public class JdbcSnapshotSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    protected Offset lowWatermark;
    protected Offset highWatermark;

    public Offset getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(Offset lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public Offset getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(Offset highWatermark) {
        this.highWatermark = highWatermark;
    }

    @Override
    public boolean isRunning() {
        return lowWatermark != null && highWatermark != null;
    }
}
