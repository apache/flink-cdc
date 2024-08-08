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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.kafka.clients.producer.ProducerRecord;

/** Partition Strategy for sending {@link ProducerRecord} to kafka partition. */
public enum PartitionStrategy {

    /** All {@link ProducerRecord} will be sent to partition 0. */
    ALL_TO_ZERO("all-to-zero"),

    /** {@link ProducerRecord} will be sent to specific partition by primary key. */
    HASH_BY_KEY("hash-by-key");

    private final String value;

    PartitionStrategy(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
