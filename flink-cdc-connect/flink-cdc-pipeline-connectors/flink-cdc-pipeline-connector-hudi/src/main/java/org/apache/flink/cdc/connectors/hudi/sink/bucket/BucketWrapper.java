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

package org.apache.flink.cdc.connectors.hudi.sink.bucket;

import org.apache.flink.cdc.common.event.Event;

import java.io.Serializable;

/**
 * Wrapper class that implements Event and associates an event with a target bucket/task index. Used
 * to enable bucket-based partitioning while allowing schema events to be broadcast.
 *
 * <p>By implementing Event, this wrapper can be transparently passed through the operator chain
 * while maintaining bidirectional communication for FlushSuccessEvent.
 */
public class BucketWrapper implements Event, Serializable {

    private static final long serialVersionUID = 1L;

    private final int bucket;
    private final Event event;

    public BucketWrapper(int bucket, Event event) {
        this.bucket = bucket;
        this.event = event;
    }

    public int getBucket() {
        return bucket;
    }

    public Event getEvent() {
        return event;
    }
}
