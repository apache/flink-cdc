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

package org.apache.flink.cdc.common.event;

import java.util.List;
import java.util.Objects;

/**
 * An {@link Event} from {@code SchemaOperator} to notify {@code DataSinkWriterOperator} that it
 * start flushing.
 */
public class FlushEvent implements Event {
    /** The sink table(s) that need to be flushed. */
    private final List<TableId> tableIds;

    /** Which subTask ID this FlushEvent was initiated from. */
    private final int sourceSubTaskId;

    /** Which type of schema change event caused this FlushEvent. */
    private final SchemaChangeEventType schemaChangeEventType;

    public FlushEvent(
            int sourceSubTaskId,
            List<TableId> tableIds,
            SchemaChangeEventType schemaChangeEventType) {
        this.tableIds = tableIds;
        this.sourceSubTaskId = sourceSubTaskId;
        this.schemaChangeEventType = schemaChangeEventType;
    }

    public List<TableId> getTableIds() {
        return tableIds;
    }

    public int getSourceSubTaskId() {
        return sourceSubTaskId;
    }

    public SchemaChangeEventType getSchemaChangeEventType() {
        return schemaChangeEventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlushEvent)) {
            return false;
        }
        FlushEvent that = (FlushEvent) o;
        return sourceSubTaskId == that.sourceSubTaskId
                && Objects.equals(tableIds, that.tableIds)
                && Objects.equals(schemaChangeEventType, that.schemaChangeEventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceSubTaskId);
    }

    @Override
    public String toString() {
        return "FlushEvent{"
                + "sourceSubTaskId="
                + sourceSubTaskId
                + ", tableIds="
                + tableIds
                + ", schemaChangeEventType="
                + schemaChangeEventType
                + '}';
    }
}
