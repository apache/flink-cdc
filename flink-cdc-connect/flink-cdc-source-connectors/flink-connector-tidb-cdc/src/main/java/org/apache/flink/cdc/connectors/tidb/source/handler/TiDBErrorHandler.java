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

package org.apache.flink.cdc.connectors.tidb.source.handler;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import com.github.shyiko.mysql.binlog.network.ServerException;
import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.pipeline.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.sql.SQLException;

public class TiDBErrorHandler extends ErrorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBErrorHandler.class);
    private static final String SQL_CODE_TOO_MANY_CONNECTIONS = "08004";

    public TiDBErrorHandler(TiDBConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(MySqlConnector.class, connectorConfig, queue);
    }

    protected boolean isRetriable(Throwable throwable) {
        LOG.info("start tidb errorHandler : {}", throwable.getClass());
        if (throwable instanceof SQLException) {
            final SQLException sql = (SQLException) throwable;
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSQLState());
        } else if (throwable instanceof ServerException) {
            final ServerException sql = (ServerException) throwable;
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSqlState());
        } else if (throwable instanceof EOFException) {
            // Retry with reading binlog error
            return throwable.getMessage().contains("Failed to read next byte from position");
        } else if (throwable instanceof DebeziumException && throwable.getCause() != null) {
            return isRetriable(throwable.getCause());
        }
        return false;
    }
}
