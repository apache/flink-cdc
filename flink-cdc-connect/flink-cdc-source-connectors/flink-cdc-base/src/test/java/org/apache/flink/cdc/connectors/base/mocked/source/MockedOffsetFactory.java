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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

/** {@link OffsetFactory} for mocked source. */
public class MockedOffsetFactory extends OffsetFactory {
    @Override
    public Offset newOffset(Map<String, String> offset) {
        return new MockedOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException(
                "Could not create new Offset by filename and position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException("Could not create new Offset by position.");
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        throw new UnsupportedOperationException("Could not create new Offset by timestamp.");
    }

    @Override
    public Offset createInitialOffset() {
        return new MockedOffset(0);
    }

    @Override
    public Offset createNoStoppingOffset() {
        return new MockedOffset(Long.MAX_VALUE);
    }
}
