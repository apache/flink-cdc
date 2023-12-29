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
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.RotateEventData;

import java.io.IOException;

/** FindOffsetListener. */
public class FindOffsetListener implements BinaryLogClient.EventListener {

    private long firstEventTs = 0L;

    private final BinaryLogClient client;

    public FindOffsetListener(BinaryLogClient client) {
        this.client = client;
    }

    public long getFirstEventTs() {
        return this.firstEventTs;
    }

    @Override
    public void onEvent(Event event) {
        EventData data = event.getData();
        if (data instanceof RotateEventData) {
            // We skip RotateEventData because it does not contain the timestamp we are interested
            // in.
            return;
        }

        EventHeaderV4 header = event.getHeader();
        long timestamp = header.getTimestamp();
        if (timestamp > 0) {
            this.firstEventTs = timestamp;
            try {
                client.disconnect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
