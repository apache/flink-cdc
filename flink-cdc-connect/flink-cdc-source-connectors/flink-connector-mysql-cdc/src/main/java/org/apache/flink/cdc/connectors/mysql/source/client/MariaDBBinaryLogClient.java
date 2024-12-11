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

package org.apache.flink.cdc.connectors.mysql.source.client;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.DumpBinaryLogCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;

import java.io.IOException;
import java.util.logging.Logger;

/** MariaDBBinaryLogClient. */
public class MariaDBBinaryLogClient extends BinaryLogClient {

    private final Logger logger = Logger.getLogger(getClass().getName());

    public MariaDBBinaryLogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    @Override
    protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
        Command dumpBinaryLogCommand;

        /*
           https://jira.mariadb.org/browse/MDEV-225
           if set @mariadb_slave_capability=1, binlogClient can not receive mariadbGtidSet.
        */
        channel.write(new QueryCommand("SET @mariadb_slave_capability=4"));
        checkError(channel.read());

        synchronized (gtidSetAccessLock) {
            if (null != gtidSet) {
                logger.info(gtidSet.toString());
                channel.write(
                        new QueryCommand(
                                "SET @slave_connect_state = '" + gtidSet.toString() + "'"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_strict_mode = 0"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_ignore_duplicates = 0"));
                checkError(channel.read());
                dumpBinaryLogCommand =
                        new DumpBinaryLogCommand(serverId, "", 0L, isUseSendAnnotateRowsEvent());
            } else {
                dumpBinaryLogCommand =
                        new DumpBinaryLogCommand(
                                serverId, getBinlogFilename(), getBinlogPosition());
            }
        }
        channel.write(dumpBinaryLogCommand);
    }
}
