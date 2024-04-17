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

package org.apache.flink.cdc.runtime.testutils.schema;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link MetadataApplier} for testing that holds all schema change events in a list for further
 * examination.
 */
public class CollectingMetadataApplier implements MetadataApplier {
    private final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();

    private final Duration duration;

    public CollectingMetadataApplier(Duration duration) {
        this.duration = duration;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        schemaChangeEvents.add(schemaChangeEvent);
        if (duration != null) {
            try {
                Thread.sleep(duration.toMillis());
            } catch (Exception ignore) {

            }
        }
    }

    public List<SchemaChangeEvent> getSchemaChangeEvents() {
        return schemaChangeEvents;
    }
}
