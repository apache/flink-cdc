/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.parser;

import com.ververica.cdc.common.event.SchemaChangeEvent;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/** A ddl parser that will use custom listener. */
public class CustomMySqlAntlrDdlParser extends MySqlAntlrDdlParser {

    private final LinkedList<SchemaChangeEvent> parsedEvents;

    public CustomMySqlAntlrDdlParser() {
        super();
        this.parsedEvents = new LinkedList<>();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new CustomMySqlAntlrDdlParserListener(this, parsedEvents);
    }

    public List<SchemaChangeEvent> getAndClearParsedEvents() {
        List<SchemaChangeEvent> result = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return result;
    }
}
