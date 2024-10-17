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

/** This is the main Oracle Antlr DDL parser. */
public class CustomOracleAntlrDdlParser extends OracleDdlParser {

    private final LinkedList<SchemaChangeEvent> parsedEvents;

    private String catalogName;
    private String schemaName;

    public CustomOracleAntlrDdlParser() {
        super();
        this.parsedEvents = new LinkedList<>();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new CustomOracleAntlrDdlParserListener(catalogName, schemaName, this, parsedEvents);
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<SchemaChangeEvent> getAndClearParsedEvents() {
        List<SchemaChangeEvent> result = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return result;
    }
}
