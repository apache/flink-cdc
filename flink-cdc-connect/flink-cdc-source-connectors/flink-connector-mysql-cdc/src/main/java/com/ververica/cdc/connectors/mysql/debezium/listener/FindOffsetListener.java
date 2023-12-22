/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.debezium.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;

/** FindOffsetListener. */
public class FindOffsetListener implements BinaryLogClient.EventListener {

    private long firstEventTs = 0L;

    public FindOffsetListener() {}

    public long getFirstEventTs() {
        return this.firstEventTs;
    }

    @Override
    public void onEvent(Event event) {
        EventHeader eventHeader = event.getHeader();
        long ts = eventHeader.getTimestamp();
        if (ts != 0) {
            this.firstEventTs = ts;
            throw sneakyThrow(new BinlogListenerThrowable());
        }
    }

    public static RuntimeException sneakyThrow(Throwable t) {
        return sneakyThrow0(t);
    }

    private static <T extends Throwable> T sneakyThrow0(Throwable t) throws T {
        throw (T) t;
    }
}
