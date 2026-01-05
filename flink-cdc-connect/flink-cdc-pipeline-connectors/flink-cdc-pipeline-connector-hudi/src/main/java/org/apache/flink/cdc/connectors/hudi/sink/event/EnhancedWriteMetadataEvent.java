/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import org.apache.hudi.sink.event.WriteMetadataEvent;

/**
 * An {@link OperatorEvent} that enhances a standard Hudi {@link WriteMetadataEvent} with additional
 * context required for multi-table sinking.
 *
 * <p>The standard {@code WriteMetadataEvent} does not contain information about which destination
 * table it belongs to. This event wraps the original event and adds the {@code tablePath}, allowing
 * the {@code MultiTableStreamWriteOperatorCoordinator} to correctly route the write metadata to the
 * timeline of the appropriate table.
 */
public class EnhancedWriteMetadataEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    /** The original event from the Hudi write function. */
    private final WriteMetadataEvent originalEvent;

    /** The filesystem path of the Hudi table this event belongs to. */
    private final String tablePath;

    /**
     * Constructs a new EnhancedWriteMetadataEvent.
     *
     * @param originalEvent The original {@link WriteMetadataEvent} from the writer.
     * @param tablePath The path of the Hudi table this metadata belongs to.
     */
    public EnhancedWriteMetadataEvent(WriteMetadataEvent originalEvent, String tablePath) {
        this.originalEvent = originalEvent;
        this.tablePath = tablePath;
    }

    /**
     * Gets the original, un-enhanced event.
     *
     * @return The original {@link WriteMetadataEvent}.
     */
    public WriteMetadataEvent getOriginalEvent() {
        return originalEvent;
    }

    /**
     * Gets the path of the Hudi table.
     *
     * @return The table path string.
     */
    public String getTablePath() {
        return tablePath;
    }

    @Override
    public String toString() {
        return "EnhancedWriteMetadataEvent{"
                + "tablePath='"
                + tablePath
                + '\''
                + ", instantTime='"
                + originalEvent.getInstantTime()
                + '\''
                + '}';
    }
}
