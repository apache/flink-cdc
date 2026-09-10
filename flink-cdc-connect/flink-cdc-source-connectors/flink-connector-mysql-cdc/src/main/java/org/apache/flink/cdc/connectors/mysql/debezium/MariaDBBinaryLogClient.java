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

package org.apache.flink.cdc.connectors.mysql.debezium;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.DumpBinaryLogCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link BinaryLogClient} for MariaDB that raises {@code @mariadb_slave_capability} from 1 -> 4
 * before requesting the binlog stream.
 *
 * <p>{@code mysql-binlog-connector-java} 0.27.2 hard-codes {@code SET @mariadb_slave_capability=1}
 * in {@code com.github.shyiko.mysql.binlog.BinaryLogClient#requestBinaryLogStreamMaria(long)}. With
 * capability 1 the MariaDB server keeps the legacy replication protocol and server emits
 * per-transaction {@code MARIADB_GTID} event, so the consumed GTID position can never advance (see
 * MariaDB MDEV-225 <a href="https://jira.mariadb.org/browse/MDEV-225">...</a>). Capability 4 makes
 * the server deliver one "MARIADB_GTID" event per transaction, which is what lets the CDC offset's
 * GTID set move forward across ckp and failover.
 *
 * <p>Only the MariaDB request path is overridden; the MySQL path is inherited unchanged.
 */
public class MariaDBBinaryLogClient extends BinaryLogClient {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDBBinaryLogClient.class);

    public MariaDBBinaryLogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    @Override
    protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
        Command dumpBinaryLogCommand;

        // https://jira.mariadb.org/browse/MDEV-225 - capability 4 (not the stock 1) is required for
        // the server to send per-txn MARIADB_GTID events, to the consumed GTID can advance.
        channel.write(new QueryCommand("SET @mariadb_slave_capability=4"));
        checkError(channel.read());

        synchronized (gtidSetAccessLock) {
            if (gtidSet != null) {
                LOG.debug("MariaDB slave_connect_state = {}", gtidSet);
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
