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

package org.apache.flink.cdc.runtime.operators.schema.common.event;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

/** Request to {@link SchemaRegistry} for getting schema of a table. */
@Internal
public class GetEvolvedSchemaRequest implements CoordinationRequest {
    public static final int LATEST_SCHEMA_VERSION = -1;

    private final TableId tableId;
    private final int schemaVersion;

    public static GetEvolvedSchemaRequest ofLatestSchema(TableId tableId) {
        return new GetEvolvedSchemaRequest(tableId, LATEST_SCHEMA_VERSION);
    }

    public GetEvolvedSchemaRequest(TableId tableId, int schemaVersion) {
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
        return "GetEvolvedSchemaRequest{"
                + "tableId="
                + tableId
                + ", schemaVersion="
                + schemaVersion
                + '}';
    }
}
