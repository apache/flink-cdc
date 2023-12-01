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

package com.ververica.cdc.common.event;

import java.util.Objects;

/**
 * An {@link Event} from {@code SchemaOperator} to notify {@code DataSinkWriterOperator} that it
 * start flushing.
 */
public class FlushEvent implements Event {

    /** The schema changes from which table. */
    private final TableId tableId;

    public FlushEvent(TableId tableId) {
        this.tableId = tableId;
    }

    public TableId getTableId() {
        return tableId;
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
        return Objects.equals(tableId, that.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId);
    }
}
