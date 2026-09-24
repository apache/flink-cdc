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

package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

import static org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset.NO_STOPPING_OFFSET;

/** The factory class for {@link EventOffset}. */
public class EventOffsetFactory extends OffsetFactory {

    @Override
    public Offset newOffset(Map<String, String> offset) {
        return new EventOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Offset newOffset(Long position) {
        return new EventOffset(position);
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        return new EventOffset(timestampMillis);
    }

    @Override
    public Offset createInitialOffset() {
        return EventOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return NO_STOPPING_OFFSET;
    }
}
