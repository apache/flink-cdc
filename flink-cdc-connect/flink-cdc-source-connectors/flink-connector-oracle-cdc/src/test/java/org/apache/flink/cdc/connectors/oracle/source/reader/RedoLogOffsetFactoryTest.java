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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RedoLogOffsetFactory}. */
class RedoLogOffsetFactoryTest {

    @Test
    void testCreateTimestampOffset() {
        long startupTimestampMillis = 1700000000000L;
        RedoLogOffsetFactory offsetFactory = new RedoLogOffsetFactory();
        RedoLogOffset offset =
                (RedoLogOffset) offsetFactory.createTimestampOffset(startupTimestampMillis);

        assertThat(offset.getScn()).isEqualTo("0");
        assertThat(offset.getStartupTimestampMillis()).isEqualTo(startupTimestampMillis);
    }
}
