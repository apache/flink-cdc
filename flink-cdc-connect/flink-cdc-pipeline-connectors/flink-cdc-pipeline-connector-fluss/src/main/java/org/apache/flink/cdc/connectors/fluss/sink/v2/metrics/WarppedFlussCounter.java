/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.fluss.sink.v2.metrics;

import org.apache.flink.metrics.Counter;

/* This file is based on source code of Apache Fluss Project (https://fluss.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An implementation of Flink's {@link Counter} which wraps Fluss's Counter. */
public class WarppedFlussCounter implements Counter {

    private final com.alibaba.fluss.metrics.Counter flussCounter;

    public WarppedFlussCounter(com.alibaba.fluss.metrics.Counter flussCounter) {
        this.flussCounter = flussCounter;
    }

    @Override
    public void inc() {
        flussCounter.inc();
    }

    @Override
    public void inc(long n) {
        flussCounter.inc(n);
    }

    @Override
    public void dec() {
        flussCounter.dec();
    }

    @Override
    public void dec(long n) {
        flussCounter.dec(n);
    }

    @Override
    public long getCount() {
        return flussCounter.getCount();
    }
}
