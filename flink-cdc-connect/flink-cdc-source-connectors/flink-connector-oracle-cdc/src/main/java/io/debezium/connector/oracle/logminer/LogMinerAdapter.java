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

package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerAdapter;

/**
 * Delegates to {@link BufferedLogMinerAdapter}.
 *
 * <p>Originally copied from Debezium 1.9.8.Final with a minor fix to the snapshot SCN comparison
 * (using {@code <=} instead of {@code <}). In Debezium 3.4.2 the adapter was refactored into {@link
 * BufferedLogMinerAdapter}; this class preserves the original type name for any remaining
 * references while delegating all behaviour to the new implementation.
 */
public class LogMinerAdapter extends BufferedLogMinerAdapter {

    public LogMinerAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }
}
