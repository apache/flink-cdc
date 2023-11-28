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

package com.ververica.cdc.runtime.partitioning;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;

/**
 * Function for unloading {@link Event} from internal {@link PartitioningEvent} after {@link
 * EventPartitioner}.
 */
@Internal
public class PostPartitionProcessor extends RichMapFunction<PartitioningEvent, Event> {
    @Override
    public Event map(PartitioningEvent value) throws Exception {
        return value.getPayload();
    }
}
