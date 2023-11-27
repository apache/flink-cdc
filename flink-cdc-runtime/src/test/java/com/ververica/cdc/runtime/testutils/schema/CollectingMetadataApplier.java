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

package com.ververica.cdc.runtime.testutils.schema;

import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.sink.MetadataApplier;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link MetadataApplier} for testing that holds all schema change events in a list for further
 * examination.
 */
public class CollectingMetadataApplier implements MetadataApplier {
    private final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        schemaChangeEvents.add(schemaChangeEvent);
    }

    public List<SchemaChangeEvent> getSchemaChangeEvents() {
        return schemaChangeEvents;
    }
}
