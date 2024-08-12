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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;

/**
 * {@code DataSink} is used to write change data to external system and apply metadata changes to
 * external systems as well.
 */
@PublicEvolving
public interface DataSink {

    /** Get the {@link EventSinkProvider} for writing changed data to external systems. */
    EventSinkProvider getEventSinkProvider();

    /** Get the {@link MetadataApplier} for applying metadata changes to external systems. */
    MetadataApplier getMetadataApplier();

    /**
     * Get the {@code HashFunctionProvider<DataChangeEvent>} for calculating hash value if you need
     * to partition by data change event before Sink.
     */
    default HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return new DefaultDataChangeEventHashFunctionProvider();
    }

    default HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider(
            int parallelism) {
        return getDataChangeEventHashFunctionProvider(); // fallback to nullary version if it isn't
        // overridden
    }
}
