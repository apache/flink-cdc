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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;

import io.debezium.connector.mysql.MySqlConnection;

/** Utils for handling {@link BinlogOffset}. */
public class BinlogOffsetUtils {

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
     *   <li>TIMESTAMP: set to earliest, as the current implementation is reading from the earliest
     *       offset and drop events earlier than the specified timestamp.
     *   <li>LATEST: fetch the current binlog by JDBC
     * </ul>
     */
    public static BinlogOffset initializeEffectiveOffset(
            BinlogOffset offset, MySqlConnection connection, MySqlSourceConfig mySqlSourceConfig) {
        BinlogOffsetKind offsetKind = offset.getOffsetKind();
        switch (offsetKind) {
            case EARLIEST:
                return BinlogOffset.ofBinlogFilePosition("", 0);
            case TIMESTAMP:
                return DebeziumUtils.findBinlogOffset(
                        offset.getTimestampSec() * 1000, connection, mySqlSourceConfig);
            case LATEST:
                return DebeziumUtils.currentBinlogOffset(connection);
            default:
                return offset;
        }
    }

    public static boolean isNonStoppingOffset(BinlogOffset binlogOffset) {
        return BinlogOffsetKind.NON_STOPPING.equals(binlogOffset.getOffsetKind());
    }
}
