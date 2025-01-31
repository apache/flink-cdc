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

package org.apache.flink.cdc.connectors.oceanbase.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

/** A change stream offset factory class that creates {@link LogMessageOffset} instances. */
public class LogMessageOffsetFactory extends OffsetFactory {

    @Override
    public LogMessageOffset newOffset(Map<String, String> offset) {
        return new LogMessageOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogMessageOffset createTimestampOffset(long timestampMillis) {
        return LogMessageOffset.from(timestampMillis);
    }

    @Override
    public LogMessageOffset createInitialOffset() {
        return LogMessageOffset.INITIAL_OFFSET;
    }

    @Override
    public LogMessageOffset createNoStoppingOffset() {
        return LogMessageOffset.NO_STOPPING_OFFSET;
    }
}
