/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.tidb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.tikv.kvproto.Kvrpcpb.KvPair;

import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the TiKV snapshot event into data types
 * (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the deserialization schema.
 */
@PublicEvolving
public interface TiKVSnapshotEventDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /** Deserialize the TiDB record. */
    void deserialize(KvPair record, Collector<T> out) throws Exception;
}
