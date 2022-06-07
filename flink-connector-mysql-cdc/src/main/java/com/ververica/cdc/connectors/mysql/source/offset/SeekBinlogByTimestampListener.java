/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.offset;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** event listener to seek binlog offset by timestamp . */
public class SeekBinlogByTimestampListener implements BinaryLogClient.EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(SeekBinlogByTimestampListener.class);

    private final long seekTimestamp;
    private final BinaryLogClient client;
    private final BinlogOffset maxBinlogOffset;
    private BinlogOffset binlogOffset;
    private BinlogOffset lastBinlogOffset;

    public SeekBinlogByTimestampListener(
            long seekTimestamp, BinaryLogClient client, BinlogOffset maxBinlogOffset) {
        this.seekTimestamp = seekTimestamp;
        this.client = client;
        this.maxBinlogOffset = maxBinlogOffset;
    }

    @Override
    public void onEvent(Event event) {
        try {
            EventHeader header = event.getHeader();
            EventType eventType = header.getEventType();
            long binlogTimestamp = header.getTimestamp();

            // Get the min binlog offset of this binlog file
            if (binlogOffset == null && eventType == EventType.ROTATE) {
                binlogOffset = buildBinlogOffset(binlogTimestamp);
                return;
            }

            // The first event binlogTimestamp > seekTimestamp , we skip this binlog file directly
            if (eventType == EventType.FORMAT_DESCRIPTION && seekTimestamp < binlogTimestamp) {
                LOG.info(
                        "skip this binlog file {} directly , binlogTimestamp = {}, seekTimestamp = {}",
                        client.getBinlogFilename(),
                        binlogTimestamp,
                        seekTimestamp);
                client.disconnect();
            }

            // up to the binlog file max position , exit it
            if (client.getBinlogPosition() >= maxBinlogOffset.getPosition()
                    && client.getBinlogFilename().equals(maxBinlogOffset.getFilename())) {
                LOG.info(
                        "up to the binlog file max position , exit it , binlogFile = {}, binlogPosition = {}",
                        client.getBinlogPosition(),
                        client.getBinlogFilename());
                client.disconnect();
            }

            if ((eventType == EventType.QUERY || eventType == EventType.XID)
                    && lastBinlogOffset != null
                    && lastBinlogOffset.getTimestamp() > 0
                    && seekTimestamp / 1000 >= lastBinlogOffset.getTimestamp()
                    && seekTimestamp <= binlogTimestamp) {
                binlogOffset = buildBinlogOffset(binlogTimestamp);
                LOG.info(
                        "found the binlog offset , lastBinlogOffset = {}, "
                                + "currentBinlogOffset = {}, seekTimestamp = {}"
                                + "",
                        lastBinlogOffset,
                        binlogOffset,
                        seekTimestamp);
                client.disconnect();
            }

            lastBinlogOffset = buildBinlogOffset(binlogTimestamp);
        } catch (Exception e) {
            LOG.info("Exception while disconnect binary log client", e);
        }
    }

    private BinlogOffset buildBinlogOffset(long binlogTimestamp) {
        return new BinlogOffset(
                client.getBinlogFilename(),
                client.getBinlogPosition(),
                0,
                0,
                binlogTimestamp / 1000,
                client.getGtidSet(),
                ((Long) client.getServerId()).intValue());
    }

    public BinlogOffset getBinlogOffset() {
        return this.binlogOffset;
    }
}
