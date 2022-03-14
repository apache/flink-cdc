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
        public boolean isKv;
        public Kvrpcpb.KvPair kvPair;
        public Cdcpb.Event.Row row;

        public TiKVRowValue(Kvrpcpb.KvPair kvPair) {
            this.isKv = true;
            this.kvPair = kvPair;
        }

        public TiKVRowValue(Cdcpb.Event.Row row) {
            this.isKv = false;
            this.row = row;
        }
    }
}
