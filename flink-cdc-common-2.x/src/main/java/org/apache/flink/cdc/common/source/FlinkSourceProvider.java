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

package org.apache.flink.cdc.common.source;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.Event;

/**
 * {@code FlinkSourceProvider} is used to provide a Flink {@link Source} for reading events from
 * external systems.
 */
@PublicEvolving
public interface FlinkSourceProvider extends EventSourceProvider {

    /** Get the {@link Source} for reading events from external systems. */
    Source<Event, ?, ?> getSource();

    /** Create a {@link FlinkSourceProvider} from a {@link Source}. */
    static FlinkSourceProvider of(Source<Event, ?, ?> source) {
        return () -> source;
    }
}
