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

package com.ververica.cdc.connectors.mysql.source.offset;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.debezium.task.SimpleBinlogReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.context.SimpleBinlogChangeEventSourceContextImpl;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import io.debezium.connector.mysql.*;
import io.debezium.pipeline.spi.OffsetContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for handling {@link BinlogOffset}. */
public class BinlogOffsetUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogOffsetUtils.class);

    /**
     * Initialize the binlog offset according to the kind of binlog offset, so that the debezium
     * reader could interpret it and seek the reader to the offset.
     *
     * <p>This method will be used in binlog reading phase, when the {@link StatefulTaskContext} is
     * being initialized to load the actual effective binlog offset.
     *
     * <p>The binlog offset kind will be overridden to {@link BinlogOffsetKind#SPECIFIC} after the
     * initialization, as the initialized effective offset describes a specific position in binlog.
     *
     * <p>Initialization strategy:
     *
     * <ul>
     *   <li>EARLIEST: binlog filename = "", position = 0
     *   <li>TIMESTAMP: set to earliest, use SimpleBinlogReadTask to find binlog position by
     *       specified timestamp, if not found, start from earliest binlog file
     *   <li>LATEST: fetch the current binlog by JDBC
     * </ul>
     */
    public static BinlogOffset initializeEffectiveOffset(
            BinlogOffset offset, StatefulTaskContext statefulTaskContext) {
        BinlogOffsetKind offsetKind = offset.getOffsetKind();
        switch (offsetKind) {
            case EARLIEST:
                return BinlogOffset.ofBinlogFilePosition("", 0);
            case TIMESTAMP:
                return seekBinlogPositionByTimestamp(offset, statefulTaskContext);
            case LATEST:
                return DebeziumUtils.currentBinlogOffset(statefulTaskContext.getConnection());
            default:
                return offset;
        }
    }

    public static boolean isNonStoppingOffset(BinlogOffset binlogOffset) {
        return BinlogOffsetKind.NON_STOPPING.equals(binlogOffset.getOffsetKind());
    }

    public static BinlogOffset seekBinlogPositionByTimestamp(
            BinlogOffset binlogOffset, StatefulTaskContext statefulTaskContext) {
        List<BinlogOffset> binlogOffsetList =
                DebeziumUtils.allBinlogFilesAndOffset(statefulTaskContext.getConnection());
        binlogOffsetList.sort((o1, o2) -> o2.getFilename().compareTo(o1.getFilename()));
        BinlogOffset currentBinlogOffset;
        for (BinlogOffset file : binlogOffsetList) {
            LOG.info("auto fetch binlog file : " + file.getFilename());
            currentBinlogOffset = BinlogOffset.ofBinlogFilePosition(file.getFilename(), 4);
            Map<String, String> seekedBinlogOffset = new HashMap<>();
            SimpleBinlogReadTask simpleBinlogReadTask =
                    new SimpleBinlogReadTask(
                            statefulTaskContext.getConnectorConfig(),
                            statefulTaskContext.getConnection(),
                            statefulTaskContext.getDispatcher(),
                            statefulTaskContext.getErrorHandler(),
                            StatefulTaskContext.getClock(),
                            statefulTaskContext.getTaskContext(),
                            (MySqlStreamingChangeEventSourceMetrics)
                                    statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                            binlogOffset.getTimestampSec() * 1000,
                            seekedBinlogOffset);
            OffsetContext.Loader<MySqlOffsetContext> loader =
                    new MySqlOffsetContext.Loader(statefulTaskContext.getConnectorConfig());
            try {
                simpleBinlogReadTask.execute(
                        new SimpleBinlogChangeEventSourceContextImpl(),
                        loader.load(currentBinlogOffset.getOffset()));
                if (StringUtils.isNotBlank(
                        seekedBinlogOffset.get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY))) {
                    LOG.info(
                            "fetch binlog position success! filename: {}, position: {}",
                            seekedBinlogOffset.get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY),
                            seekedBinlogOffset.get(BinlogOffset.BINLOG_POSITION_OFFSET_KEY));
                    return BinlogOffset.ofBinlogFilePosition(
                            seekedBinlogOffset.get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY),
                            Long.valueOf(
                                    seekedBinlogOffset.get(
                                            BinlogOffset.BINLOG_POSITION_OFFSET_KEY)));
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Execute binlog read task for seek binlog position by timestamp fail", e);
            }
        }
        // if not find binlog position, start read binlog from earliest binlog file
        LOG.warn("not find binlog file and position by timestamp, start from earliest binlog file");
        return BinlogOffset.ofBinlogFilePosition("", 0);
    }
}
