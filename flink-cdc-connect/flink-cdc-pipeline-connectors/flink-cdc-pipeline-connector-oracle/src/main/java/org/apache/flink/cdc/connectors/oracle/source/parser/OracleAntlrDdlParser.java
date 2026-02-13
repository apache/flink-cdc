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

package org.apache.flink.cdc.connectors.oracle.source.parser;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.connector.oracle.antlr.OracleDdlParser;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/** A ddl parser that will use custom listener. */
public class OracleAntlrDdlParser extends OracleDdlParser {

    private final LinkedList<SchemaChangeEvent> parsedEvents;

    private final String databaseName;
    private final String schemaName;

    public OracleAntlrDdlParser(String databaseName, String schemaName) {
        super();
        this.parsedEvents = new LinkedList<>();
        this.databaseName = databaseName;
        this.schemaName = schemaName;
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new OracleAntlrDdlParserListener(
                this.databaseName, this.schemaName, this, parsedEvents);
    }

    public List<SchemaChangeEvent> getAndClearParsedEvents() {
        List<SchemaChangeEvent> result = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return result;
    }
}
