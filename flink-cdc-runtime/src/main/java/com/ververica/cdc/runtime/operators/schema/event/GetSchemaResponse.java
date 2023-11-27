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

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Coordination response from {@link
 * com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry} for {@link
 * GetSchemaRequest}.
 */
@Internal
public class GetSchemaResponse implements CoordinationResponse {
    @Nullable private final Schema schema;

    public GetSchemaResponse(@Nullable Schema schema) {
        this.schema = schema;
    }

    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }
}
