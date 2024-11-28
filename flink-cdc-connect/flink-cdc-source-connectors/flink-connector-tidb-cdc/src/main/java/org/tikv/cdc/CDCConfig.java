package org.tikv.cdc;

import org.tikv.kvproto.Kvrpcpb;

public class CDCConfig {
    private static final int EVENT_BUFFER_SIZE = 50000;
    private static final int MAX_ROW_KEY_SIZE = 10240;
    private static final boolean READ_OLD_VALUE = true;
    private int eventBufferSize = 50000;
    private int maxRowKeySize = 10240;
    private boolean readOldValue = true;

    public CDCConfig() {}

    public void setEventBufferSize(int bufferSize) {
        this.eventBufferSize = bufferSize;
    }

    public void setMaxRowKeySize(int rowKeySize) {
        this.maxRowKeySize = rowKeySize;
    }

    public void setReadOldValue(boolean value) {
        this.readOldValue = value;
    }

    public int getEventBufferSize() {
        return this.eventBufferSize;
    }

    public int getMaxRowKeySize() {
        return this.maxRowKeySize;
    }

    public boolean getReadOldValue() {
        return this.readOldValue;
    }

    Kvrpcpb.ExtraOp getExtraOp() {
        return this.readOldValue ? Kvrpcpb.ExtraOp.ReadOldValue : Kvrpcpb.ExtraOp.Noop;
    }
}
