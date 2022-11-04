/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.cdc;

import org.tikv.kvproto.Kvrpcpb;

public class CDCConfig {
    private static final int EVENT_BUFFER_SIZE = 50000;
    private static final int MAX_ROW_KEY_SIZE = 10240;
    private static final boolean READ_OLD_VALUE = true;

    private int eventBufferSize = EVENT_BUFFER_SIZE;
    private int maxRowKeySize = MAX_ROW_KEY_SIZE;
    private boolean readOldValue = READ_OLD_VALUE;

    public void setEventBufferSize(final int bufferSize) {
        eventBufferSize = bufferSize;
    }

    public void setMaxRowKeySize(final int rowKeySize) {
        maxRowKeySize = rowKeySize;
    }

    public void setReadOldValue(final boolean value) {
        readOldValue = value;
    }

    public int getEventBufferSize() {
        return eventBufferSize;
    }

    public int getMaxRowKeySize() {
        return maxRowKeySize;
    }

    public boolean getReadOldValue() {
        return readOldValue;
    }

    Kvrpcpb.ExtraOp getExtraOp() {
        return readOldValue ? Kvrpcpb.ExtraOp.ReadOldValue : Kvrpcpb.ExtraOp.Noop;
    }
}
