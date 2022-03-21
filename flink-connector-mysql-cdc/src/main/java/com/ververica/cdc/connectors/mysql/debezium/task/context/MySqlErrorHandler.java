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

package com.ververica.cdc.connectors.mysql.debezium.task.context;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A subclass implementation of {@link ErrorHandler} which filter some {@link DebeziumException}, we
 * use this class instead of {@link io.debezium.connector.mysql.MySqlErrorHandler}.
 */
public class MySqlErrorHandler extends ErrorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlErrorHandler.class);
    private static final Pattern NOT_FOUND_TABLE_MSG_PATTERN =
            Pattern.compile(
                    "Encountered change event for table (.+)\\.(.+) whose schema isn't known to this connector");

    MySqlTaskContext context;

    public MySqlErrorHandler(
            String logicalName, ChangeEventQueue<?> queue, MySqlTaskContext context) {
        super(MySqlConnector.class, logicalName, queue);
        this.context = context;
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        return false;
    }

    @Override
    public void setProducerThrowable(Throwable producerThrowable) {
        if (producerThrowable.getCause() instanceof DebeziumException) {
            DebeziumException e = (DebeziumException) producerThrowable.getCause();
            String detailMessage = e.getMessage();
            Matcher matcher = NOT_FOUND_TABLE_MSG_PATTERN.matcher(detailMessage);
            if (matcher.find()) {
                String databaseName = matcher.group(1);
                String tableName = matcher.group(2);
                TableId tableId = new TableId(databaseName, null, tableName);
                if (context.getSchema().schemaFor(tableId) == null) {
                    LOG.warn("Schema for table " + tableId + " is null");
                    return;
                }
            }
        }
        super.setProducerThrowable(producerThrowable);
    }
}
