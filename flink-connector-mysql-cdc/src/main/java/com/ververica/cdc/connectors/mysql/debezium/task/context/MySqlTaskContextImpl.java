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

package com.ververica.cdc.connectors.mysql.debezium.task.context;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTaskContext;

/** A subclass implementation of {@link MySqlTaskContext} which reuses one BinaryLogClient. */
public class MySqlTaskContextImpl extends MySqlTaskContext {

    private final BinaryLogClient reusedBinaryLogClient;

    public MySqlTaskContextImpl(
            MySqlConnectorConfig config,
            MySqlDatabaseSchema schema,
            BinaryLogClient reusedBinaryLogClient) {
        super(config, schema);
        this.reusedBinaryLogClient = reusedBinaryLogClient;
    }

    @Override
    public BinaryLogClient getBinaryLogClient() {
        return reusedBinaryLogClient;
    }
}
