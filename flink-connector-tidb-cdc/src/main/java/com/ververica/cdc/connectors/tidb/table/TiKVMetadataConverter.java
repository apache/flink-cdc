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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.annotation.Internal;

import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.io.Serializable;

/** A converter converts TiKV Row metadata into Flink internal data structures. */
@FunctionalInterface
@Internal
public interface TiKVMetadataConverter extends Serializable {

    Object read(TiKVRowValue row);

    /** TiKV Row Value. */
    class TiKVRowValue {
        public boolean isSnapshotRecord;
        public Kvrpcpb.KvPair kvPair;
        public Cdcpb.Event.Row row;

        public TiKVRowValue(Kvrpcpb.KvPair kvPair) {
            this.isSnapshotRecord = true;
            this.kvPair = kvPair;
        }

        public TiKVRowValue(Cdcpb.Event.Row row) {
            this.isSnapshotRecord = false;
            this.row = row;
        }
    }
}
