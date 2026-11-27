/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.fluss.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Reports the reader's restored buckets at startup, including an empty set for new readers.
 *
 * <p>This is a temporary protocol until the source adopts FLIP-537. It rebuilds the enumerator's
 * assignment view without adding checkpoint state or moving restored splits between readers.
 */
public class ReaderSplitReportEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Set<TableBucket> buckets;

    public ReaderSplitReportEvent(Set<TableBucket> buckets) {
        this.buckets = Collections.unmodifiableSet(new HashSet<>(buckets));
    }

    public Set<TableBucket> getBuckets() {
        return buckets;
    }
}
