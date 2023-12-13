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

package org.apache.flink.cdc.connectors.db2.source.handler;

import org.apache.flink.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;

import io.debezium.schema.SchemaChangeEvent;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

import static io.debezium.connector.db2.SourceInfo.CHANGE_LSN_KEY;
import static io.debezium.connector.db2.SourceInfo.COMMIT_LSN_KEY;

/**
 * This Db2SchemaChangeEventHandler helps to parse the source struct in SchemaChangeEvent and
 * generate source info.
 */
public class Db2SchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        Map<String, Object> source = new HashMap<>();
        Struct sourceInfo = event.getSource();
        String changeLsn = sourceInfo.getString(CHANGE_LSN_KEY);
        String commitLsn = sourceInfo.getString(COMMIT_LSN_KEY);
        source.put(CHANGE_LSN_KEY, changeLsn);
        source.put(COMMIT_LSN_KEY, commitLsn);
        return source;
    }
}
