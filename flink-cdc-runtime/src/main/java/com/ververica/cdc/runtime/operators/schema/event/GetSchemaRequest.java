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

package com.ververica.cdc.runtime.operators.schema.event;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.TableId;

/**
 * Request to {@link com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry} for
 * getting schema of a table.
 */
@Internal
public class GetSchemaRequest implements CoordinationRequest {
    public static final int LATEST_SCHEMA_VERSION = -1;

    private final TableId tableId;
    private final int schemaVersion;

    public static GetSchemaRequest ofLatestSchema(TableId tableId) {
        return new GetSchemaRequest(tableId, LATEST_SCHEMA_VERSION);
    }

    public GetSchemaRequest(TableId tableId, int schemaVersion) {
        this.tableId = tableId;
        this.schemaVersion = schemaVersion;
    }

    public TableId getTableId() {
        return tableId;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public String toString() {
        return "GetSchemaRequest{"
                + "tableId="
                + tableId
                + ", schemaVersion="
                + schemaVersion
                + '}';
    }
}
